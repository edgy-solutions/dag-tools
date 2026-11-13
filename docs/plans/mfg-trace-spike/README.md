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
