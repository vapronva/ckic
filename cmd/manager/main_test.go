package main

import (
	"reflect"
	"testing"
)

func TestExternalEndpointsDeduplicatePerNode(t *testing.T) {
	t.Parallel()
	got, err := parseExternalEndpoints([]string{
		"node1=1.1.1.1,1.1.1.1, 2.2.2.2", "node1=2.2.2.2", "node2=1.1.1.1",
	})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string][]string{"node1": {"1.1.1.1", "2.2.2.2"}, "node2": {"1.1.1.1"}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("endpoints = %v, want %v", got, want)
	}
}
