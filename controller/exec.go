package controller

import (
	"context"
	"io"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
)

func ExecCmdInPod(ctx context.Context, client kubernetes.Interface, restConfig rest.Config, namespace, podName, containerName string, command []string) error {
	req := client.CoreV1().RESTClient().Post().Resource("pods").Name(podName).
		Namespace(namespace).SubResource("exec")
	req.VersionedParams(
		runtime.Object(&corev1.PodExecOptions{
			Stdin:     false,
			Stdout:    true,
			Stderr:    true,
			TTY:       false,
			Container: containerName,
			Command:   command,
		}),
		scheme.ParameterCodec,
	)
	option := &remotecommand.StreamOptions{
		Stdout: io.Discard,
		Stderr: io.Discard,
	}
	exec, err := remotecommand.NewSPDYExecutor(&restConfig, "POST", req.URL())
	if err != nil {
		return err
	}
	return exec.StreamWithContext(ctx, *option)
}
