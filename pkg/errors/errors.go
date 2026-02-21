package errors

import "fmt"

type ConfigurationFailedError struct {
	NodeName string
	Reason   string
	Err      error
}

func (e *ConfigurationFailedError) Error() string {
	if e == nil {
		return "failed to update configuration: <nil pointer>"
	}
	if e.Err == nil {
		return fmt.Sprintf("failed to update configuration on node %s: %s", e.NodeName, e.Reason)
	}
	return fmt.Sprintf(
		"failed to update configuration on node %s: %s: %v",
		e.NodeName,
		e.Reason,
		e.Err,
	)
}

func (e *ConfigurationFailedError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}
