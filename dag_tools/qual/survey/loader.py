"""Load ``Definitions`` instances from a workspace.yaml or module path,
capturing every warning the load emits.

The recipe rule (Phase 1, item 1.2):

  > Load the Definitions / repository ... with ``-W all``, capturing all
  > deprecation/experimental warnings. A load failure fails the Jenkins
  > stage and nothing is published — the registry never contains an
  > inventory for code that doesn't load.

So the loader's job is:

  1. Accept either a path to ``workspace.yaml`` or a module-spec string
     ``pkg.mod:attr`` (matching Dagster's own conventions).
  2. Load every code location and return a list of
     ``LoadResult`` (name, source, defs OR error) tuples.
  3. Capture every Python warning emitted during the load — we expose
     them as ``WarningRecord``s so the survey can publish them in
     ``load_validation.json`` and humans can see what's drifting.
"""
from __future__ import annotations

import importlib
import importlib.util
import logging
import sys
import traceback
import warnings
from dataclasses import dataclass
from pathlib import Path
from typing import Any, List, Optional

import yaml

from .schemas import LoadFailure, LoadedLocation, WarningRecord


logger = logging.getLogger(__name__)


@dataclass
class LoadResult:
    """One code location's load outcome.

    Either ``defs`` is set (success) OR ``error`` is set (failure). Both
    are never set at once. ``warnings_captured`` is the per-location
    subset of warnings emitted while loading this specific location.
    """
    name: str
    source: str
    defs: Optional[Any] = None
    error: Optional[str] = None
    traceback: Optional[str] = None
    warnings_captured: List[WarningRecord] = None  # type: ignore[assignment]

    def __post_init__(self):
        if self.warnings_captured is None:
            self.warnings_captured = []

    @property
    def loaded(self) -> bool:
        return self.defs is not None and self.error is None

    def to_loaded_location(self, **counts: int) -> LoadedLocation:
        return LoadedLocation(name=self.name, source=self.source, **counts)

    def to_failure(self) -> LoadFailure:
        return LoadFailure(
            name=self.name,
            source=self.source,
            error=self.error or "<unknown>",
            traceback=self.traceback,
        )


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def load_locations(spec: str) -> List[LoadResult]:
    """Load every code location referenced by ``spec``.

    ``spec`` is either:

      * A path to a workspace.yaml file — every ``load_from`` entry is loaded
        as a separate ``LoadResult``.
      * A Python module spec ``pkg.mod:attr`` — single LoadResult.
      * A path to a ``.py`` file — single LoadResult, attribute discovered.

    Never raises: a failed location is recorded as a ``LoadResult`` with
    ``error`` set. The caller decides whether to publish or refuse.
    """
    spec = spec.strip()
    spec_path = Path(spec)

    if spec_path.suffix in (".yaml", ".yml") and spec_path.exists():
        return _load_from_workspace_yaml(spec_path)
    if spec_path.suffix == ".py" and spec_path.exists():
        return [_load_python_file(name=spec_path.stem, file=spec_path, attr=None)]
    if ":" in spec or "." in spec:
        # Treat as module spec; allow optional :attr.
        module_name, _, attr = spec.partition(":")
        return [_load_python_module(name=module_name, module_name=module_name, attr=attr or None)]

    # Bare token that doesn't look like a path or module — return as failure.
    return [LoadResult(
        name=spec,
        source=spec,
        error=f"unrecognized locations spec: {spec!r}; expected workspace.yaml, *.py, or pkg.mod[:attr]",
    )]


# ---------------------------------------------------------------------------
# Workspace.yaml parser
# ---------------------------------------------------------------------------


def _load_from_workspace_yaml(path: Path) -> List[LoadResult]:
    try:
        with path.open() as f:
            doc = yaml.safe_load(f) or {}
    except Exception as e:
        return [LoadResult(
            name=path.name, source=str(path),
            error=f"failed to parse workspace yaml: {e}",
            traceback=traceback.format_exc(),
        )]

    load_from = doc.get("load_from") or []
    if not load_from:
        return [LoadResult(
            name=path.name, source=str(path),
            error="workspace.yaml has no 'load_from' entries",
        )]

    results: List[LoadResult] = []
    workspace_dir = path.parent.resolve()

    for idx, entry in enumerate(load_from):
        if not isinstance(entry, dict):
            results.append(LoadResult(
                name=f"entry-{idx}", source=str(path),
                error=f"load_from[{idx}] is not a mapping",
            ))
            continue

        if "python_file" in entry:
            cfg = entry["python_file"] or {}
            rel = cfg.get("relative_path") or cfg.get("file") or ""
            if not rel:
                results.append(LoadResult(
                    name=f"python_file-{idx}", source=str(path),
                    error="python_file entry has no relative_path/file",
                ))
                continue
            wd = (workspace_dir / (cfg.get("working_directory") or ".")).resolve()
            file_path = (workspace_dir / rel).resolve()
            attr = cfg.get("attribute")
            with _temporary_sys_path(str(wd)):
                results.append(_load_python_file(
                    name=cfg.get("location_name") or file_path.stem,
                    file=file_path,
                    attr=attr,
                ))

        elif "python_module" in entry:
            cfg = entry["python_module"] or {}
            module_name = cfg.get("module_name")
            if not module_name:
                results.append(LoadResult(
                    name=f"python_module-{idx}", source=str(path),
                    error="python_module entry has no module_name",
                ))
                continue
            attr = cfg.get("attribute")
            results.append(_load_python_module(
                name=cfg.get("location_name") or module_name,
                module_name=module_name,
                attr=attr,
            ))

        else:
            results.append(LoadResult(
                name=f"entry-{idx}", source=str(path),
                error=f"unsupported load_from entry keys: {sorted(entry.keys())}",
            ))

    return results


# ---------------------------------------------------------------------------
# Python file / module loaders (with warning capture)
# ---------------------------------------------------------------------------


def _load_python_file(name: str, file: Path, attr: Optional[str]) -> LoadResult:
    if not file.exists():
        return LoadResult(name=name, source=str(file), error=f"file not found: {file}")
    try:
        captured: List[WarningRecord] = []
        with _capture_warnings(into=captured, location=name):
            module_spec_name = f"__dagtools_loaded_{name}__"
            spec = importlib.util.spec_from_file_location(module_spec_name, str(file))
            if spec is None or spec.loader is None:
                return LoadResult(
                    name=name, source=str(file),
                    error=f"could not build importlib spec for {file}",
                )
            module = importlib.util.module_from_spec(spec)
            sys.modules[module_spec_name] = module
            try:
                spec.loader.exec_module(module)
            finally:
                # Don't pollute sys.modules with the throwaway name even on
                # success — the survey is a one-shot run.
                sys.modules.pop(module_spec_name, None)

        defs = _resolve_defs_attribute(module, attr)
        if defs is None:
            return LoadResult(
                name=name, source=str(file),
                error="could not locate a Definitions instance in module",
                warnings_captured=captured,
            )
        return LoadResult(
            name=name, source=str(file), defs=defs, warnings_captured=captured,
        )
    except Exception as e:
        return LoadResult(
            name=name, source=str(file),
            error=f"{type(e).__name__}: {e}",
            traceback=traceback.format_exc(),
        )


def _load_python_module(name: str, module_name: str, attr: Optional[str]) -> LoadResult:
    try:
        captured: List[WarningRecord] = []
        with _capture_warnings(into=captured, location=name):
            module = importlib.import_module(module_name)
        defs = _resolve_defs_attribute(module, attr)
        if defs is None:
            return LoadResult(
                name=name, source=module_name,
                error="could not locate a Definitions instance in module",
                warnings_captured=captured,
            )
        return LoadResult(
            name=name, source=module_name, defs=defs, warnings_captured=captured,
        )
    except Exception as e:
        return LoadResult(
            name=name, source=module_name,
            error=f"{type(e).__name__}: {e}",
            traceback=traceback.format_exc(),
        )


def _resolve_defs_attribute(module: Any, attr: Optional[str]) -> Any:
    """Find a Dagster ``Definitions`` instance on a freshly-loaded module.

    Resolution order:

      1. Explicit ``attr`` argument (workspace.yaml ``attribute`` field).
      2. Module attribute literally named ``defs`` (the prevailing convention).
      3. Module attribute literally named ``definitions``.
      4. First attribute whose type's qualified name is ``Definitions``.

    Returns the matched object, or None.
    """
    if attr:
        return getattr(module, attr, None)
    for candidate in ("defs", "definitions"):
        obj = getattr(module, candidate, None)
        if obj is not None and _is_definitions(obj):
            return obj
    for attr_name in dir(module):
        if attr_name.startswith("_"):
            continue
        obj = getattr(module, attr_name, None)
        if obj is not None and _is_definitions(obj):
            return obj
    return None


def _is_definitions(obj: Any) -> bool:
    """Duck-typed check: ``type(obj).__qualname__ == "Definitions"`` and
    the type lives under the ``dagster`` package."""
    t = type(obj)
    if t.__qualname__ != "Definitions":
        return False
    module_name = getattr(t, "__module__", "") or ""
    return module_name.startswith("dagster")


# ---------------------------------------------------------------------------
# Warning capture + sys.path scoping
# ---------------------------------------------------------------------------


class _capture_warnings:
    """Context manager that captures every warning emitted inside it as
    ``WarningRecord`` entries appended to ``into``.

    Equivalent to invoking Python with ``-W all`` — every warning is
    surfaced exactly once, regardless of previously-installed filters.
    """

    def __init__(self, into: List[WarningRecord], location: Optional[str] = None):
        self._into = into
        self._location = location
        self._saved_filters = None

    def __enter__(self):
        self._ctx = warnings.catch_warnings(record=True)
        self._records = self._ctx.__enter__()
        # -W all
        warnings.resetwarnings()
        warnings.simplefilter("always")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        for w in self._records:
            cat = w.category
            cat_fqn = f"{cat.__module__}.{cat.__qualname__}" if cat else "Warning"
            self._into.append(WarningRecord(
                category=cat_fqn,
                message=str(w.message),
                filename=str(w.filename) if w.filename else None,
                lineno=int(w.lineno) if w.lineno else None,
                location=self._location,
            ))
        return self._ctx.__exit__(exc_type, exc_val, exc_tb)


class _temporary_sys_path:
    """Temporarily prepend a directory to ``sys.path`` during a ``with`` block.

    Mirrors the Dagster workspace loader's behavior: a python_file's
    ``working_directory`` is added to sys.path so its sibling imports
    resolve correctly.
    """

    def __init__(self, directory: str):
        self._dir = directory
        self._added = False

    def __enter__(self):
        if self._dir not in sys.path:
            sys.path.insert(0, self._dir)
            self._added = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._added:
            try:
                sys.path.remove(self._dir)
            except ValueError:
                pass
        return False
