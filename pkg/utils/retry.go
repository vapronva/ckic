package utils

import (
	"fmt"
	"time"
)

func WithRetry(operation func() error, maxRetries int, delay time.Duration) error {
	var err error
	for range maxRetries {
		if err = operation(); err == nil {
			return nil
		}
		time.Sleep(delay)
	}
	return fmt.Errorf("operation failed after %d retries: %w", maxRetries, err)
}
