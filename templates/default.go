package templates

const DefaultCaddyfileTemplate = `
{{- with .Ingress }}
{{- range .Spec.Rules }}
{{ .Host }} {
  {{- if .HTTP }}
    {{- range .HTTP.Paths }}
      route {{ .Path }} {
        reverse_proxy {{ .Backend.Service.Name }}:{{ service_port .Backend.Service.Name .Backend.Service.Port }}
      }
    {{- end }}
  {{- else }}
      # WARN: no HTTP rule defined for this host
  {{- end }}
}
{{- end }}
{{- end }}
`
