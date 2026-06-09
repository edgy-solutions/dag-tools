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
