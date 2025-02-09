package templates

const DefaultCaddyfileTemplate = `
{{- with .Ingress }}
  {{- $namespace := .Namespace }}
  {{- range .Spec.Rules }}
{{ .Host }}:443 {
  {{- if .HTTP }}
  import basic-compression-set
  import default-headers-set-custom
  header * {
    X-Robots-Tag "noindex, noarchive, nofollow"
  }
    {{- if gt (len .HTTP.Paths) 0 }}
      {{- if eq (len .HTTP.Paths) 1 }}
        {{- $path := index .HTTP.Paths 0 }}
  import reverse-proxy-xri-hd {{ printf "%s.%s.svc.cluster.local:%d" $path.Backend.Service.Name $namespace $path.Backend.Service.Port.Number }}
      {{- else }}
        {{- range .HTTP.Paths }}
  route {{ .Path }} {
    import reverse-proxy-xri-hd {{ printf "%s.%s.svc.cluster.local:%d" .Backend.Service.Name $namespace .Backend.Service.Port.Number }}
  }
        {{- end }}
      {{- end }}
    {{- else }}
  # WARN: HTTP rule is present for host {{ .Host }} but no paths have been configured
    {{- end }}
  {{- else }}
  # WARN: no HTTP rule defined for this host
  {{- end }}
}
  {{- end }}
{{- end }}
`
