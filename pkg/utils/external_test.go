package utils_test

import (
	"reflect"
	"testing"

	"git.horse/vapronva/ckic/pkg/utils"
)

func TestExternalEndpointsDeduplicatePerNode(t *testing.T) {
	t.Parallel()
	got, err := utils.ParseExternalEndpoints([]string{
		"node1=1.1.1.1,1.1.1.1, 2.2.2.2", "node1=2.2.2.2", "node2=1.1.1.1",
	})
	if err != nil {
		t.Fatal(err)
	}
	want := utils.ExternalEndpointsMap{"node1": {"1.1.1.1", "2.2.2.2"}, "node2": {"1.1.1.1"}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("endpoints = %v, want %v", got, want)
	}
}
