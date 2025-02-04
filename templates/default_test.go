package templates_test

import (
	"bytes"
	"testing"
	"text/template"

	"github.com/stretchr/testify/assert"
	"gl.vprw.ru/vapronva/caddy-kubernetes-ingress-controller/templates"
)

func TestDefaultCaddyfileTemplate_ParseAndExecute(t *testing.T) {
	tpl, err := template.New("test-default").Parse(templates.DefaultCaddyfileTemplate)
	assert.NoError(t, err, "default template should parse without errors")
	var buf bytes.Buffer
	dummyData := struct {
		Ingress interface{}
	}{
		Ingress: nil,
	}
	err = tpl.Execute(&buf, dummyData)
	assert.NoError(t, err, "executing default template with nil ingress data shouldn't error")
}
