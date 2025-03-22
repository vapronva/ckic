package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/pflag"

	"gl.vprw.ru/vapronva/ckic/pkg/controller"
	"gl.vprw.ru/vapronva/ckic/pkg/utils"
)

func main() {
	kubeconfigPath := pflag.String("kubeconfig", "", "Path to kubeconfig file")
	nodeLabel := pflag.String("node-label", "ckic.cmld.ru/enabled", "Node label to watch for")
	configMapName := pflag.String("config-map", "caddy-config", "ConfigMap containing Caddy configuration")
	configMapNamespace := pflag.String("config-namespace", "caddy-system", "Namespace of the ConfigMap and deployments")
	communicationMethod := pflag.String("comm-method", "clusterip", "Communication method (clusterip or direct)")
	logLevel := pflag.String("log-level", "info", "Log level (debug, info, warn, error)")
	caddyImage := pflag.String("caddy-image", "rg.gl.vprw.ru/oss-images/zerossl-caddy/caddy:2.9.1-alpine", "Caddy image (format image:tag)")
	enableNodePort := pflag.Bool("enable-nodeport", false, "Enable NodePort service exposure")
	pflag.Parse()
	level, err := zerolog.ParseLevel(*logLevel)
	if err != nil {
		level = zerolog.InfoLevel
	}
	utils.SetupLogger(level)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-signalCh
		log.Info().Str("signal", sig.String()).Msg("Received signal, shutting down")
		cancel()
		time.Sleep(5 * time.Second)
		os.Exit(0)
	}()
	c, err := controller.NewController(ctx, controller.ControllerConfig{
		Kubeconfig:          *kubeconfigPath,
		NodeLabel:           *nodeLabel,
		ConfigMapName:       *configMapName,
		ConfigMapNamespace:  *configMapNamespace,
		CommunicationMethod: *communicationMethod,
		CaddyImage:          *caddyImage,
		EnableNodePort:      *enableNodePort,
	})
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create controller")
	}
	log.Info().Msg("Starting CKIC manager")
	if err := c.Run(ctx); err != nil {
		log.Fatal().Err(err).Msg("Controller exited with error")
	}
}
