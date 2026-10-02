{{/*
Common labels applied to every resource in the chart.
*/}}
{{- define "dag-tools.labels" -}}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end }}

{{/*
Per-component selector labels.
Usage: include "dag-tools.selectorLabels" (dict "component" "restate-worker" "root" .)
*/}}
{{- define "dag-tools.selectorLabels" -}}
app.kubernetes.io/name: {{ .root.Chart.Name }}
app.kubernetes.io/component: {{ .component }}
{{- end }}

{{/*
Optional cluster-domain suffix appended to in-cluster Service hostnames.
*/}}
{{- define "dag-tools.svcDomain" -}}
{{- .Values.global.clusterDomain | default "" -}}
{{- end }}

{{/*
Resolve a full image path for a chart-owned image (restate-worker, central-gateway).
Usage: include "dag-tools.image" (dict "name" "restate-worker" "image" .Values.restateWorker.image "root" .)
*/}}
{{- define "dag-tools.image" -}}
{{- $tag := .image.tag | default .root.Chart.AppVersion -}}
{{- if .image.repository -}}
{{ .root.Values.global.imageRegistry }}/{{ .image.repository }}:{{ $tag }}
{{- else -}}
{{ .root.Values.global.imageRegistry }}/{{ .root.Values.global.imagePrefix }}/{{ .name }}:{{ $tag }}
{{- end -}}
{{- end }}

{{/*
Full image path for the upstream Restate server (defaults to docker.io/restatedev/restate).
*/}}
{{- define "dag-tools.restateImage" -}}
{{- $tag := .Values.restateServer.image.tag | default "latest" -}}
{{ .Values.restateServer.image.registry }}/{{ .Values.restateServer.image.repository }}:{{ $tag }}
{{- end }}

{{/*
Restate admin URL — resolves to the in-chart Service when restateServer.enabled,
otherwise to externalRestate.adminUrl. Fails template rendering with a clear
message if neither is configured.
*/}}
{{- define "dag-tools.restateAdminUrl" -}}
{{- if .Values.restateServer.enabled -}}
http://{{ .Release.Name }}-restate{{ include "dag-tools.svcDomain" . }}:{{ .Values.restateServer.adminPort }}
{{- else if .Values.externalRestate.adminUrl -}}
{{ .Values.externalRestate.adminUrl }}
{{- else -}}
{{- fail "Either restateServer.enabled must be true or externalRestate.adminUrl must be set." -}}
{{- end -}}
{{- end }}

{{/*
Restate ingress URL — same in-chart / external resolution.
*/}}
{{- define "dag-tools.restateIngressUrl" -}}
{{- if .Values.restateServer.enabled -}}
http://{{ .Release.Name }}-restate{{ include "dag-tools.svcDomain" . }}:{{ .Values.restateServer.ingressPort }}
{{- else if .Values.externalRestate.ingressUrl -}}
{{ .Values.externalRestate.ingressUrl }}
{{- else -}}
{{- fail "Either restateServer.enabled must be true or externalRestate.ingressUrl must be set." -}}
{{- end -}}
{{- end }}

{{/*
The advertised URI Restate uses to call back to the worker. Defaults to the
in-chart Service DNS unless restateWorker.advertisedUri is explicitly set.
*/}}
{{- define "dag-tools.workerAdvertisedUri" -}}
{{- if .Values.restateWorker.advertisedUri -}}
{{ .Values.restateWorker.advertisedUri }}
{{- else -}}
http://{{ .Release.Name }}-restate-worker{{ include "dag-tools.svcDomain" . }}:{{ .Values.restateWorker.port }}
{{- end -}}
{{- end }}

{{/*
----------------------------------------------------------------------------
Extra CA certificates, for endpoints served with a private or self-signed CA.
----------------------------------------------------------------------------
Shared by every component that makes outbound TLS calls. Two measured facts
drive the shape (the measurements are pinned in
dag_tools_tests/test_chart_ca_certs.py):

  * The code uses BOTH `requests` and `httpx`, and they disagree about
    which env var names a CA bundle. `requests` reads REQUESTS_CA_BUNDLE
    and ignores SSL_CERT_FILE; `httpx` reads SSL_CERT_FILE and ignores
    REQUESTS_CA_BUNDLE. Setting only one leaves half the call sites
    rejecting the endpoint -- including worker self-registration, which
    goes through httpx, so the worker would never serve at all.

  * Both env vars REPLACE the default trust store; they do not add to it.
    Pointing them straight at a private CA breaks every PUBLIC endpoint
    (measured: httpx to pypi.org is rejected). So an init container
    concatenates the image's public roots with the operator's certs into
    one bundle on an emptyDir, and the env vars name that merged file.

Usage, with the component's own caCerts block:
  include "dag-tools.caCerts.volumes" (dict "ca" .Values.restateWorker.caCerts "root" . "component" "restate-worker")
*/}}

{{- define "dag-tools.caCerts.dir" -}}/etc/dag-tools/ca{{- end }}
{{- define "dag-tools.caCerts.sourceDir" -}}/etc/dag-tools/ca-source{{- end }}
{{- define "dag-tools.caCerts.bundlePath" -}}{{ include "dag-tools.caCerts.dir" . }}/ca-bundle.crt{{- end }}

{{/*
The name of the chart-owned ConfigMap holding inline certs.
*/}}
{{- define "dag-tools.caCerts.inlineConfigMapName" -}}
{{ .root.Release.Name }}-{{ .component }}-ca-certs
{{- end }}

{{/*
Validate the source and return nothing. Exactly one of secretName /
configMapName / inline must be given -- a half-configured block would
otherwise mount an empty directory, and the init container's failure
("no PEM files were mounted") is a worse place to learn it than here.
*/}}
{{- define "dag-tools.caCerts.validate" -}}
{{- $n := 0 -}}
{{- if .ca.secretName }}{{ $n = add1 $n }}{{ end -}}
{{- if .ca.configMapName }}{{ $n = add1 $n }}{{ end -}}
{{- if .ca.inline }}{{ $n = add1 $n }}{{ end -}}
{{- if eq $n 0 -}}
{{- fail (printf "%s.caCerts.enabled is true but no source is set. Set exactly one of caCerts.secretName, caCerts.configMapName or caCerts.inline." .component) -}}
{{- end -}}
{{- if gt $n 1 -}}
{{- fail (printf "%s.caCerts: set exactly one of secretName, configMapName or inline -- %d were given, and only one directory is mounted." .component $n) -}}
{{- end -}}
{{- end }}

{{/*
Volumes: the operator's certs (read-only) plus the emptyDir the merged
bundle is written to.
*/}}
{{- define "dag-tools.caCerts.volumes" -}}
{{- if .ca.enabled -}}
{{- include "dag-tools.caCerts.validate" . -}}
- name: ca-certs-source
  {{- if .ca.secretName }}
  secret:
    secretName: {{ .ca.secretName | quote }}
    {{- with .ca.items }}
    items:
      {{- toYaml . | nindent 6 }}
    {{- end }}
  {{- else if .ca.configMapName }}
  configMap:
    name: {{ .ca.configMapName | quote }}
    {{- with .ca.items }}
    items:
      {{- toYaml . | nindent 6 }}
    {{- end }}
  {{- else }}
  configMap:
    name: {{ include "dag-tools.caCerts.inlineConfigMapName" . | quote }}
  {{- end }}
- name: ca-certs-bundle
  emptyDir: {}
{{- end -}}
{{- end }}

{{/*
Mounts for the APPLICATION container: the merged bundle only. The source
directory is deliberately not mounted here -- nothing in the app reads it,
and leaving it out keeps the two uses of the word "bundle" distinct.
*/}}
{{- define "dag-tools.caCerts.volumeMounts" -}}
{{- if .ca.enabled -}}
- name: ca-certs-bundle
  mountPath: {{ include "dag-tools.caCerts.dir" . | quote }}
  readOnly: true
{{- end -}}
{{- end }}

{{/*
Env vars pointing every TLS client at the merged bundle.

CURL_CA_BUNDLE is included for two reasons: `requests` falls back to it,
and it makes `kubectl exec ... curl https://endpoint` behave the same way
the handler does, which is how an operator confirms the cert is right
without redeploying.
*/}}
{{- define "dag-tools.caCerts.env" -}}
{{- if .ca.enabled -}}
- name: SSL_CERT_FILE
  value: {{ include "dag-tools.caCerts.bundlePath" . | quote }}
- name: REQUESTS_CA_BUNDLE
  value: {{ include "dag-tools.caCerts.bundlePath" . | quote }}
- name: CURL_CA_BUNDLE
  value: {{ include "dag-tools.caCerts.bundlePath" . | quote }}
{{- end -}}
{{- end }}

{{/*
The init container that builds the merged bundle. Runs the component's own
image, so it adds no pull and no second thing to version.

It EXITS NON-ZERO when the mounted source holds no PEM file. A worker that
starts with a bundle identical to the default looks configured and still
rejects the endpoint; CrashLoopBackOff naming the empty mount is the
cheaper failure.
*/}}
{{- define "dag-tools.caCerts.initContainer" -}}
{{- if .ca.enabled -}}
- name: ca-certs-merge
  image: {{ .image | quote }}
  imagePullPolicy: {{ .pullPolicy }}
  command: ["/bin/sh", "-c"]
  args:
    - |
      set -eu
      out="{{ include "dag-tools.caCerts.bundlePath" . }}"
      : > "$out"
      # Public roots FIRST: these env vars replace the default trust
      # store, so anything omitted here stops being trusted.
      for f in /etc/ssl/certs/ca-certificates.crt; do
        [ -f "$f" ] && cat "$f" >> "$out" || true
      done
      # certifi ships its own copy and is what requests/httpx use by
      # default; include it so turning this on cannot narrow trust.
      # Duplicate roots across the two files are harmless to OpenSSL.
      c=$(python -c 'import certifi; print(certifi.where())' 2>/dev/null || true)
      if [ -n "${c:-}" ] && [ -f "$c" ]; then cat "$c" >> "$out"; fi
      n=0
      for f in {{ include "dag-tools.caCerts.sourceDir" . }}/*; do
        if [ -f "$f" ]; then
          printf '\n' >> "$out"
          cat "$f" >> "$out"
          n=$((n+1))
        fi
      done
      if [ "$n" -eq 0 ]; then
        echo "caCerts is enabled but no files were mounted at {{ include "dag-tools.caCerts.sourceDir" . }}." >&2
        echo "Check that the Secret/ConfigMap exists in this namespace and holds PEM data." >&2
        exit 1
      fi
      echo "merged $n extra CA file(s) into $out ($(wc -c < "$out") bytes)"
  volumeMounts:
    - name: ca-certs-source
      mountPath: {{ include "dag-tools.caCerts.sourceDir" . | quote }}
      readOnly: true
    - name: ca-certs-bundle
      mountPath: {{ include "dag-tools.caCerts.dir" . | quote }}
{{- end -}}
{{- end }}
