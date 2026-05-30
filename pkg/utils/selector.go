package utils

import (
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/labels"
)

func NormalizeNodeLabelSelector(selector string) (string, labels.Selector, error) {
	selector = strings.TrimSpace(selector)
	if selector == "" {
		return "", labels.Everything(), nil
	}
	parsed, err := labels.Parse(selector)
	if err != nil {
		return "", nil, fmt.Errorf("invalid node label selector %q: %w", selector, err)
	}
	return parsed.String(), parsed, nil
}
