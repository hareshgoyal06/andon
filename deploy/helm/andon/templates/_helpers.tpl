{{- define "andon.image" -}}
{{- $reg := .root.Values.image.registry -}}
{{- if $reg }}{{ $reg }}/{{ end }}andon-{{ .name }}:{{ .root.Values.image.tag }}
{{- end -}}

{{- define "andon.labels" -}}
app.kubernetes.io/part-of: andon
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "andon.stations" -}}
{{- $n := int .Values.plcSim.stations -}}
{{- $base := int .Values.plcSim.basePort -}}
{{- $out := list -}}
{{- range $i := until $n -}}
{{- $out = append $out (printf "station-%d=plc-sim:%d" (add1 $i) (add $base $i)) -}}
{{- end -}}
{{ join "," $out }}
{{- end -}}
