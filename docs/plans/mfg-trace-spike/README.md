# SystemLink Test Monitor spike probe

Read-only probe that answers the open questions in `docs/plans/mfg-trace.md` section 3
against a real server. It writes one JSON file, which is reviewed and then committed
here as `<server-version>.json`.

## Running it

Minimal component file (synthetic values; the shape PR A will use):

```yaml
type: dag_tools.DltPipelineComponent
attributes:
  source_config:
    type: systemlink_test_monitor
    source_id: test_repo   # PR A requires it; the probe ignores it
    connection:
      base_url: "{{ env('SYSTEMLINK_BASE_URL') }}"
      api_key_env: SYSTEMLINK_API_KEY
      # api_version_path: /nitestmonitor/v2
      # workspace_id: ws-example
      # verify_tls: true
```

```
export SYSTEMLINK_BASE_URL=https://systemlink.example.invalid
export SYSTEMLINK_API_KEY=...
.venv/Scripts/python.exe -m dag_tools.systemlink.spike \
    --component component.yaml --part-number PN-1000-A
```

Without `--part-number` only the Swagger observations are recorded. Other options:
`--out-dir`, `--swagger-path` (repeatable), `--pages`, `--page-take`, `--overwrite`.
Exit codes: 0 ok, 2 configuration or missing document, 3 the server answered 401.

## What it records

Route paths, field names and enum spellings from the Swagger document; booleans, HTTP
status codes and counts from a few live queries; the route name and status of each request.

## What it never records

The base URL or host, the API key, the part number, workspace id, and any result id,
step id, serial, program name, status, measurement or timestamp from a response.
Request bodies are not recorded. Filters use placeholders with substitutions; no value
is ever written into a filter string.

It can only send `GET <swagger path>` and `POST` to `query-results`, `query-steps` and
`query-products`; any other route raises before a request is made.

## Before committing

Read the JSON. It should hold structure and counts only. If anything looks like data,
do not commit it; report it.

## At work, in one paste (PowerShell)

Fill in the three `<...>` values. The probe needs only `httpx` and `pyyaml`, so
`uv run --no-project` skips the library's heavy dependencies. The component file goes in
`%TEMP%`, outside the repo, and the key exists only in this shell's environment.

```powershell
git fetch origin; git switch docs/mfg-trace-spec-and-spike; git pull --ff-only
$env:SYSTEMLINK_BASE_URL = "<https://your-systemlink-host>"
$env:SYSTEMLINK_API_KEY  = "<api key>"
$pn = "<a part number that has test results>"
$cfg = Join-Path $env:TEMP "sl-spike.yaml"
[IO.File]::WriteAllText($cfg, @'
type: dag_tools.DltPipelineComponent
attributes:
  source_config:
    type: systemlink_test_monitor
    source_id: test_repo
    connection:
      base_url: "{{ env('SYSTEMLINK_BASE_URL') }}"
      api_key_env: SYSTEMLINK_API_KEY
'@)
uv run --no-project --with httpx --with pyyaml python -m dag_tools.systemlink.spike --component $cfg --part-number $pn > $null
$out = Get-ChildItem docs\plans\mfg-trace-spike\*.json | Sort-Object LastWriteTime | Select-Object -Last 1
$hostName = ([uri]$env:SYSTEMLINK_BASE_URL).Host
"--- leak check (must print nothing):"
Select-String -Path $out.FullName -SimpleMatch -Pattern $hostName, $pn, $env:SYSTEMLINK_API_KEY
"--- wrote $($out.FullName)"
```

- **Exit codes:** 2 is a config problem or no Swagger document, and 3 means the server
  answered 401 (the key).
- **Private CA:** if the server uses a corporate CA, set `$env:SSL_CERT_FILE` to a bundle that
  contains it. Don't turn TLS off.
- **Workspaces:** if results live in one workspace, add `workspace_id: <id>` under
  `connection:`. The id is never recorded.

**Reading it before committing.**
- The leak check must print nothing.
- Then open the file. Every value under `observations` should be one of:
  - a route path;
  - an enum spelling;
  - `true` / `false`;
  - an HTTP status;
  - a count (`n_steps`, `pages_walked`, ...);
  - or `null` with a `detail`.
- `requests` lists only a route name and a status per call.
- Look for a serial, a result id, a program name, a timestamp or a hostname. If you find one,
  **don't commit**. Send the line instead.
- Should the host appear anyway, redact it in the committed copy before `git add`:
  `[IO.File]::WriteAllText($out.FullName, [IO.File]::ReadAllText($out.FullName).Replace($hostName, "systemlink.example.invalid"))`.

Then commit just that file:
`git add $out.FullName; git commit -m "docs(mfg-trace): SystemLink spike output"; git push`.
