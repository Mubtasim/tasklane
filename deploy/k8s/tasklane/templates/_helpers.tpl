{{- define "tasklane.labels" -}}
app.kubernetes.io/name: tasklane
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- end }}

{{- define "tasklane.serverSelector" -}}
app.kubernetes.io/name: tasklane
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: server
{{- end }}

{{- define "tasklane.envoySelector" -}}
app.kubernetes.io/name: tasklane
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: envoy
{{- end }}

{{- define "tasklane.secretName" -}}
{{- .Values.existingSecret | default (printf "%s-server" .Release.Name) -}}
{{- end }}

{{- define "tasklane.image" -}}
{{- printf "%s:%s" .Values.image.repository (.Values.image.tag | default .Chart.AppVersion) -}}
{{- end }}

{{- define "tasklane.headless" -}}
{{- printf "%s-headless.%s.svc.cluster.local" .Release.Name .Release.Namespace -}}
{{- end }}
