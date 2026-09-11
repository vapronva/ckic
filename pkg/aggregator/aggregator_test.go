package aggregator_test

import (
	"strings"
	"sync/atomic"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"git.horse/vapronva/ckic/pkg/aggregator"
	"git.horse/vapronva/ckic/pkg/constants"
)

func TestCurrentMergedSortsExternalsAfterBase(t *testing.T) {
	t.Parallel()
	agg := aggregator.New(nil, "caddy-system", "", nil)
	if _, err := agg.CurrentMerged(); err == nil {
		t.Fatal("missing base was accepted")
	}
	agg.UpdateBase(":80 {\n  respond \"base\"\n}")
	agg.SetExternal("team-b/cm", "b.example.com {\n  respond \"b\"\n}")
	agg.SetExternal("team-a/cm", "a.example.com {\n  respond \"a\"\n}")
	agg.SetExternal("team-c/cm", "   ")
	snapshot, err := agg.CurrentMerged()
	if err != nil {
		t.Fatal(err)
	}
	merged := snapshot.Caddyfile
	if !strings.HasPrefix(merged, ":80 {\n  respond \"base\"\n}\n") {
		t.Fatalf("base not followed by newline:\n%s", merged)
	}
	aIdx := strings.Index(merged, "# ---- Begin external from team-a/cm ----")
	bIdx := strings.Index(merged, "# ---- Begin external from team-b/cm ----")
	if aIdx == -1 || bIdx == -1 || aIdx > bIdx {
		t.Fatalf("externals missing or unsorted:\n%s", merged)
	}
	if strings.Contains(merged, "team-c/cm") {
		t.Fatalf("whitespace-only fragment was included:\n%s", merged)
	}
}

func TestBootMirrorKeepsAcceptedSnapshot(t *testing.T) {
	t.Parallel()
	client := fake.NewClientset()
	agg := aggregator.New(client, "system", "accepted", nil)
	const bootstrap = "{\n\tadmin :2019\n}\n"
	if err := agg.InitializeMirror(t.Context(), bootstrap); err != nil {
		t.Fatal(err)
	}
	checkMirror := func(want string) {
		t.Helper()
		mirror, err := client.CoreV1().ConfigMaps("system").Get(t.Context(), "accepted", metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if got := mirror.Data[constants.CaddyfileKey]; got != want {
			t.Fatalf("boot snapshot = %q, want %q", got, want)
		}
	}
	updateBase := func(base string) aggregator.Snapshot {
		t.Helper()
		agg.UpdateBase(base)
		snapshot, err := agg.CurrentMerged()
		if err != nil {
			t.Fatal(err)
		}
		return snapshot
	}
	checkMirror(bootstrap)
	candidate := updateBase("candidate\n")
	if err := agg.PublishMirror(t.Context()); err != nil {
		t.Fatal(err)
	}
	checkMirror(bootstrap)
	if err := agg.PublishAccepted(t.Context(), candidate); err != nil {
		t.Fatal(err)
	}
	checkMirror("candidate\n")
	actions := len(client.Actions())
	if err := agg.PublishAccepted(t.Context(), candidate); err != nil {
		t.Fatal(err)
	}
	if len(client.Actions()) != actions {
		t.Fatal("unchanged accepted snapshot was republished")
	}
	newer := updateBase("newer\n")
	updateBase("rejected-by-caddy\n")
	if err := agg.PublishAccepted(t.Context(), newer); err != nil {
		t.Fatal(err)
	}
	checkMirror("newer\n")
	if err := agg.PublishAccepted(t.Context(), candidate); err != nil {
		t.Fatal(err)
	}
	checkMirror("newer\n")
	restarted := aggregator.New(client, "system", "accepted", nil)
	if err := restarted.InitializeMirror(t.Context(), bootstrap); err != nil {
		t.Fatal(err)
	}
	restarted.UpdateBase("invalid\n")
	if err := restarted.PublishMirror(t.Context()); err != nil {
		t.Fatal(err)
	}
	checkMirror("newer\n")
	if err := client.CoreV1().ConfigMaps("system").Delete(t.Context(), "accepted", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := restarted.PublishMirror(t.Context()); err != nil {
		t.Fatal(err)
	}
	checkMirror("newer\n")
}

func TestBootMirrorRepairsMissingCaddyfile(t *testing.T) {
	t.Parallel()
	for _, data := range []map[string]string{nil, {constants.CaddyfileKey: ""}, {constants.CaddyfileKey: " \n"}} {
		client := fake.NewClientset(&corev1.ConfigMap{Name: "accepted", Namespace: "system", Data: data})
		agg := aggregator.New(client, "system", "accepted", nil)
		const bootstrap = "{\n\tadmin :2019\n}\n"
		if err := agg.InitializeMirror(t.Context(), bootstrap); err != nil {
			t.Fatal(err)
		}
		mirror, err := client.CoreV1().ConfigMaps("system").Get(t.Context(), "accepted", metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if got := mirror.Data[constants.CaddyfileKey]; got != bootstrap {
			t.Fatalf("boot snapshot = %q, want %q", got, bootstrap)
		}
	}
}

func TestEnqueueFiresOnlyOnChange(t *testing.T) {
	t.Parallel()
	var enqueues atomic.Int32
	agg := aggregator.New(nil, "caddy-system", "", func() { enqueues.Add(1) })
	steps := []struct {
		name string
		do   func()
		want int32
	}{
		{"base set", func() { agg.UpdateBase("a") }, 1},
		{"base unchanged", func() { agg.UpdateBase("a") }, 1},
		{"external set", func() { agg.SetExternal("ns/cm", "frag") }, 2},
		{"external unchanged", func() { agg.SetExternal("ns/cm", "frag") }, 2},
		{"external removed", func() { agg.RemoveExternal("ns/cm") }, 3},
		{"external remove missing", func() { agg.RemoveExternal("ns/cm") }, 3},
	}
	for _, step := range steps {
		step.do()
		if got := enqueues.Load(); got != step.want {
			t.Fatalf("%s: enqueues = %d, want %d", step.name, got, step.want)
		}
	}
}
