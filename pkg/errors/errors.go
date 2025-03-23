package errors

import "fmt"

type ErrConfigurationFailed struct {
	NodeName string
	Reason   string
	Err      error
}

func (e *ErrConfigurationFailed) Error() string {
	return fmt.Sprintf("failed to update configuration on node %s: %s: %v",
		e.NodeName, e.Reason, e.Err)
}

func (e *ErrConfigurationFailed) Unwrap() error {
	return e.Err
}
