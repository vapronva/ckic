package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"gl.vprw.ru/vapronva/caddy-kubernetes-ingress-controller/watcher"
	"golang.org/x/sync/errgroup"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

var (
	namespace           string
	containerAnnotation string
)

func main() {
	flag.StringVar(&namespace, "namespace", "", "namespace to edit and create configmaps")
	flag.StringVar(&containerAnnotation, "container-annotation", "caddy-kubernetes-ingress-controller/caddy-instance", "annotation to use to determine which caddy instance to update")
	flag.Parse()
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	runtime.ErrorHandlers = []runtime.ErrorHandler{
		func(ctx context.Context, err error, msg string, _ ...interface{}) {
			log.Warn().Err(err).Msgf("[k8s] %s", msg)
		},
	}
	client, err := kubernetes.NewForConfig(getKubernetesConfig())
	if err != nil {
		log.Fatal().Err(err).Msg("failed to create kubernetes client")
	}
	w := watcher.New(client, func(payload *watcher.Payload) {
		log.Info().Msg(fmt.Sprintf("received payload: %v", payload))
	})
	eg, ctx := errgroup.WithContext(context.Background())
	eg.Go(func() error {
		log.Info().Msg(fmt.Sprintf("starting watcher with context: %v", ctx))
		return w.Run(ctx)
	})
	if err := eg.Wait(); err != nil {
		log.Fatal().Err(err).Send()
	}
}

func getKubernetesConfig() *rest.Config {
	config, err := rest.InClusterConfig()
	if err != nil {
		config, err = clientcmd.BuildConfigFromFlags("", filepath.Join(os.Getenv("HOME"), ".kube", "config"))
	}
	if err != nil {
		log.Fatal().Err(err).Msg("failed to get kubernetes configuration")
	}
	return config
}
