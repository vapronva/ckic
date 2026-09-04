package aggregator_test

import (
	"strings"
	"sync/atomic"
	"testing"

	"git.horse/vapronva/ckic/pkg/aggregator"
)

func TestCurrentMergedSortsExternalsAfterBase(t *testing.T) {
	t.Parallel()
	agg := aggregator.New(nil, "caddy-system", false, "", nil)
	if _, err := agg.CurrentMerged(); err == nil {
		t.Fatal("missing base was accepted")
	}
	agg.UpdateBase(":80 {\n  respond \"base\"\n}")
	agg.SetExternal("team-b/cm", "b.example.com {\n  respond \"b\"\n}")
	agg.SetExternal("team-a/cm", "a.example.com {\n  respond \"a\"\n}")
	agg.SetExternal("team-c/cm", "   ")
	merged, err := agg.CurrentMerged()
	if err != nil {
		t.Fatal(err)
	}
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

func TestEnqueueFiresOnlyOnChange(t *testing.T) {
	t.Parallel()
	var enqueues atomic.Int32
	agg := aggregator.New(nil, "caddy-system", false, "", func() { enqueues.Add(1) })
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
