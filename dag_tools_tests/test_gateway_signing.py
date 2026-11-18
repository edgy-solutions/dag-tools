"""The gateway image must be able to verify a signed token, and must say so when it cannot.

The fault these tests seal: PyJWT's asymmetric algorithms live in its optional ``crypto``
extra. Installed without it, ``import jwt`` still succeeds, the gateway still starts and still
passes its healthcheck, only the HMAC algorithms register, and every token is refused as
invalid whatever its signature -- while PyJWT's own explanation (``RS256 requires
'cryptography' to be installed``) is swallowed by the verifier's ``except Exception``. So the
image reports a deployment fault in a token fault's words.

The load-bearing test here is the CONTROL: a check that passes in an environment where
everything is installed proves nothing about an image where it is not. The control builds the
broken state and asserts the check refuses it.
"""
import json
import logging
import os
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
CHECK = REPO / "scripts" / "check_gateway_signing.py"
DOCKERFILE = REPO / "dag_tools" / "central_gateway" / "Dockerfile"
WORKFLOW = REPO / ".github" / "workflows" / "build-containers.yml"
PYPROJECT = REPO / "pyproject.toml"

_QUOTES = chr(39) + chr(34)
_BACKSLASH = chr(92)

# PyJWT guards its cryptography imports with ``except ModuleNotFoundError: pass``. A stub
# raising a plain ImportError therefore escapes the guard and kills ``import jwt`` outright --
# which simulates a state no image is ever in, and would make this control pass for the wrong
# reason. ModuleNotFoundError is a SUBCLASS of ImportError, not the reverse.
_STUB = 'raise ModuleNotFoundError("No module named cryptography", name="cryptography")\n'


@pytest.fixture(scope="module")
def without_signature_support(tmp_path_factory):
    """A PYTHONPATH entry that makes ``cryptography`` unimportable, as the broken image is."""
    root = tmp_path_factory.mktemp("nocrypto")
    package = root / "cryptography"
    package.mkdir()
    (package / "__init__.py").write_text(_STUB, encoding="utf-8")
    return str(root)


def _run(args, extra_path=None):
    env = dict(os.environ)
    if extra_path:
        env["PYTHONPATH"] = extra_path
    else:
        env.pop("PYTHONPATH", None)
    return subprocess.run(
        [sys.executable, *args], capture_output=True, text=True, env=env, cwd=str(REPO)
    )


def _steps():
    yaml = pytest.importorskip("yaml")
    return yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))["jobs"]["build"]["steps"]


# ---------------------------------------------------------------------------
# The check, and the control that gives it meaning
# ---------------------------------------------------------------------------

def test_a_signed_token_verifies_in_this_environment():
    """The pass arm. Without it the control below would pass on a check that always fails."""
    done = _run([str(CHECK)])
    assert done.returncode == 0, done.stdout + done.stderr


def test_control_an_image_without_signature_support_fails_the_check(without_signature_support):
    done = _run([str(CHECK)], extra_path=without_signature_support)
    assert done.returncode == 1, done.stdout + done.stderr
    assert "pyjwt[crypto]" in done.stderr
    assert "Dockerfile" in done.stderr


def test_the_stub_reproduces_the_real_broken_state_rather_than_a_crash(without_signature_support):
    """The control is only evidence if its environment is the one the image is really in.

    That state is specific: the token library imports fine and reports a reduced algorithm set.
    A stub that broke ``import jwt`` would fail the check too, for a reason no image ever has.
    """
    probe = textwrap.dedent(
        """
        import json, jwt, jwt.algorithms
        print(json.dumps({
            "imported": True,
            "has_crypto": bool(getattr(jwt.algorithms, "has_crypto", False)),
            "algorithms": sorted(jwt.algorithms.get_default_algorithms()),
        }))
        """
    )
    done = _run(["-c", probe], extra_path=without_signature_support)
    assert done.returncode == 0, done.stdout + done.stderr
    state = json.loads(done.stdout)
    assert state["imported"] is True
    assert state["has_crypto"] is False
    assert "RS256" not in state["algorithms"]
    assert "HS256" in state["algorithms"]


def test_the_check_exercises_both_paths_the_gateway_can_take():
    """A JWKS-resolved key and a static PEM fail DIFFERENTLY, so one path is not coverage.

    Resolving through ``PyJWK`` raises ``MissingCryptographyError``; a static PEM raises
    ``InvalidAlgorithmError``. A check covering one would pass on an image broken for the other.
    """
    source = CHECK.read_text(encoding="utf-8")
    assert "PyJWK(" in source
    assert "to_jwk" in source
    assert "public_pem" in source


def test_the_check_needs_no_network_and_carries_no_key_material():
    source = CHECK.read_text(encoding="utf-8")
    for forbidden in ("requests", "httpx", "urllib", "socket", "PRIVATE KEY", "BEGIN "):
        assert forbidden not in source, forbidden
    assert "generate_private_key" in source  # minted per run, in memory, never written down


def test_the_check_reports_every_problem_at_once(without_signature_support):
    """Whoever is fixing an image wants the list, not the first line of it."""
    done = _run([str(CHECK)], extra_path=without_signature_support)
    assert done.stderr.count("  - ") >= 2, done.stderr


# ---------------------------------------------------------------------------
# The two places the dependency is declared
# ---------------------------------------------------------------------------

def test_the_gateway_dockerfile_installs_the_token_library_with_its_extra():
    """This list IS the image's manifest: the build context never reads the project file."""
    installed = []
    for line in DOCKERFILE.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line.startswith("#"):
            continue
        installed.append(line.rstrip(_BACKSLASH).strip().strip(_QUOTES))
    assert "pyjwt[crypto]" in installed
    assert "pyjwt" not in installed


def test_the_broker_extra_declares_the_token_library_with_its_extra():
    text = PYPROJECT.read_text(encoding="utf-8")
    assert '"PyJWT[crypto]>=' in text
    assert '"PyJWT>=' not in text


def test_the_extra_is_what_carries_signature_support():
    """Canary on PyJWT's own metadata: a conditional requirement is the whole premise here.

    If a future PyJWT makes cryptography unconditional, this fails and both comments above go
    stale -- which is the point of pinning it to the library rather than asserting it in prose.
    """
    from importlib.metadata import requires

    declared = requires("pyjwt") or []
    conditional = [
        r for r in declared
        if r.lower().startswith("cryptography") and "extra" in r and "crypto" in r
    ]
    assert conditional, declared


# ---------------------------------------------------------------------------
# CI runs it against the BUILT image
# ---------------------------------------------------------------------------

def test_the_workflow_runs_the_check_against_the_built_gateway_image():
    steps = _steps()
    loaded = [s for s in steps if (s.get("with") or {}).get("load") is True]
    assert len(loaded) == 1, [s.get("name") for s in loaded]
    assert "central-gateway" in loaded[0]["if"]
    runners = [s for s in steps if "check_gateway_signing.py" in (s.get("run") or "")]
    assert len(runners) == 1, [s.get("name") for s in steps]
    assert "central-gateway" in runners[0]["if"]
    assert "docker run" in runners[0]["run"]
    assert "--network none" in runners[0]["run"]


def test_the_check_runs_before_the_push():
    """An image that cannot verify a token must not reach the registry."""
    steps = _steps()
    names = [s.get("name") or "" for s in steps]
    checked = next(
        i for i, s in enumerate(steps) if "check_gateway_signing.py" in (s.get("run") or "")
    )
    pushed = names.index("Build and push")
    assert checked < pushed, names


def test_no_credential_is_added_to_this_workflow():
    """Standing rule for this repo's CI, re-asserted because this change touched the workflow."""
    named = {ref.split("}")[0].strip() for ref in WORKFLOW.read_text(encoding="utf-8").split("secrets.")[1:]}
    assert named == {"GITHUB_TOKEN"}, named


# ---------------------------------------------------------------------------
# What the gateway says when it is the broken image
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _reset_latch():
    from dag_tools.central_gateway import identity

    identity._no_signature_support_said = None
    yield
    identity._no_signature_support_said = None


class _FakeAlgorithms:
    def __init__(self, available):
        self._available = list(available)
        self.requires_cryptography = ["RS256", "RS384", "RS512"]

    def get_default_algorithms(self):
        return {a: object() for a in self._available}


class _FakeJwt:
    def __init__(self, available):
        self.algorithms = _FakeAlgorithms(available)


def test_a_configured_algorithm_the_library_cannot_verify_is_named(monkeypatch, caplog):
    from dag_tools.central_gateway import identity

    monkeypatch.setenv("GATEWAY_JWT_ALGORITHMS", "RS256")
    with caplog.at_level(logging.WARNING):
        identity._say_no_signature_support(_FakeJwt(["HS256", "HS384"]))
    spoken = [r for r in caplog.records if "RS256" in r.getMessage()]
    assert len(spoken) == 1, caplog.text
    assert spoken[0].levelno == logging.ERROR  # nothing configured can verify anything
    message = spoken[0].getMessage()
    assert "pyjwt[crypto]" in message
    assert "HS256" in message  # what IS available, so the state is readable from the line


def test_an_image_that_can_verify_says_nothing(monkeypatch, caplog):
    from dag_tools.central_gateway import identity

    monkeypatch.setenv("GATEWAY_JWT_ALGORITHMS", "RS256")
    with caplog.at_level(logging.WARNING):
        identity._say_no_signature_support(_FakeJwt(["RS256", "HS256"]))
    assert caplog.records == []


def test_one_configured_algorithm_of_several_is_a_warning_not_an_error(monkeypatch, caplog):
    """Some tokens still verify, so this is not the outage the all-missing case is."""
    from dag_tools.central_gateway import identity

    monkeypatch.setenv("GATEWAY_JWT_ALGORITHMS", "RS256,HS256")
    with caplog.at_level(logging.WARNING):
        identity._say_no_signature_support(_FakeJwt(["HS256"]))
    assert [r.levelno for r in caplog.records] == [logging.WARNING]


def test_it_is_said_once_per_state_not_once_per_token(monkeypatch, caplog):
    from dag_tools.central_gateway import identity

    monkeypatch.setenv("GATEWAY_JWT_ALGORITHMS", "RS256")
    broken = _FakeJwt(["HS256"])
    with caplog.at_level(logging.WARNING):
        for _ in range(5):
            identity._say_no_signature_support(broken)
    assert len(caplog.records) == 1, caplog.text


def test_control_an_image_fixed_and_then_regressed_says_so_again(monkeypatch, caplog):
    """Latched on the STATE, like `_say_unconfigured` -- not on having spoken once."""
    from dag_tools.central_gateway import identity

    monkeypatch.setenv("GATEWAY_JWT_ALGORITHMS", "RS256")
    with caplog.at_level(logging.WARNING):
        identity._say_no_signature_support(_FakeJwt(["HS256"]))
        identity._say_no_signature_support(_FakeJwt(["RS256"]))
        identity._say_no_signature_support(_FakeJwt(["HS256"]))
    assert len(caplog.records) == 2, caplog.text


def test_the_line_carries_no_token_because_it_is_not_about_one(monkeypatch, caplog):
    """This fault is a property of the image, so naming a token would misdirect the reader."""
    from dag_tools.central_gateway import identity

    monkeypatch.setenv("GATEWAY_JWT_ALGORITHMS", "RS256")
    with caplog.at_level(logging.WARNING):
        identity._say_no_signature_support(_FakeJwt(["HS256"]))
    assert "token" in caplog.text  # it does say tokens are refused...
    for leak in ("eyJ", "Bearer "):  # ...but quotes none of them
        assert leak not in caplog.text


def test_an_instrument_may_never_break_the_thing_it_measures(monkeypatch, caplog):
    """A library shaped differently than expected must cost a log line, never a refusal."""
    from dag_tools.central_gateway import identity

    class _Odd:
        algorithms = object()  # no get_default_algorithms

    monkeypatch.setenv("GATEWAY_JWT_ALGORITHMS", "RS256")
    with caplog.at_level(logging.WARNING):
        assert identity._say_no_signature_support(_Odd()) is None


def test_the_refusal_reason_is_deliberately_unchanged(monkeypatch, caplog):
    """The reason vocabulary is a separate decision -- see the function's own docstring.

    Every reason token is a count on ``/health`` and an HTTP status, so moving this case from a
    token fault to a deployment fault is a change to that vocabulary rather than to this log.
    What is sealed here is that the log was ADDED and nothing else moved: an operator still
    gets ``token_invalid``, and now also gets the sentence saying why every token gets it.
    """
    from dag_tools.central_gateway import identity

    monkeypatch.setenv("GATEWAY_JWT_ISSUERS", "https://issuer.invalid/realms/r")
    monkeypatch.setenv("GATEWAY_JWT_AUDIENCES", "aud")
    monkeypatch.setenv("GATEWAY_JWT_PUBLIC_KEY", "-not-a-key-")
    monkeypatch.setenv("GATEWAY_JWT_ALGORITHMS", "RS256")

    class _Refusing(_FakeJwt):
        def decode(self, *a, **k):
            raise ValueError("Algorithm not supported")

    monkeypatch.setitem(sys.modules, "jwt", _Refusing(["HS256"]))
    with caplog.at_level(logging.WARNING):
        claims, reason = identity._verify("header.payload.signature")

    assert claims is None
    assert reason == identity.TOKEN_INVALID
    assert reason in identity.REASONS
    assert "RS256" in caplog.text
