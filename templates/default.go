package templates

const DefaultCaddyfileTemplate = `
{{- range .Ingresses }}
{{- $ing := .Ingress }}
{{- range $rule := $ing.Spec.Rules }}
{{ $rule.Host }} {
{{- if $rule.HTTP }}
    {{- range $path := $rule.HTTP.Paths }}
	route {{ $path.Path }} {
		reverse_proxy {{ $path.Backend.Service.Name }}:{{ $path.Backend.Service.Port.Number }}
	}
    {{- end }}
{{- else }}
	# WANR: no HTTP specified for this rule
{{- end }}
}

{{- end }}
{{- end }}
`
