package controller

import (
	"context"
	"time"

	"github.com/rs/zerolog/log"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
)

type ConfigHandler func(string)

type ConfigWatcher struct {
	clientset           *kubernetes.Clientset
	namespace           string
	configMapName       string
	configHandler       ConfigHandler
	lastResourceVersion string
}

func NewConfigWatcher(ctx context.Context, clientset *kubernetes.Clientset, namespace, configMapName string, handler ConfigHandler) (*ConfigWatcher, error) {
	return &ConfigWatcher{
		clientset:     clientset,
		namespace:     namespace,
		configMapName: configMapName,
		configHandler: handler,
	}, nil
}

func (w *ConfigWatcher) Start(ctx context.Context) {
	logger := log.With().
		Str("component", "config_watcher").
		Str("namespace", w.namespace).
		Str("configmap", w.configMapName).
		Logger()

	logger.Info().Msg("Starting config watcher")
	configMap, err := w.clientset.CoreV1().ConfigMaps(w.namespace).Get(ctx, w.configMapName, metav1.GetOptions{})
	if err != nil {
		logger.Error().Err(err).Msg("Failed to get initial ConfigMap")
	} else {
		caddyfileData, exists := configMap.Data["Caddyfile"]
		if exists {
			logger.Info().Msg("Initial ConfigMap loaded, notifying handlers")
			w.configHandler(caddyfileData)
		} else {
			logger.Warn().Msg("ConfigMap does not contain Caddyfile key")
		}
		w.lastResourceVersion = configMap.ResourceVersion
	}
	for {
		select {
		case <-ctx.Done():
			logger.Info().Msg("Config watcher shutting down")
			return
		default:
			watcher, err := w.clientset.CoreV1().ConfigMaps(w.namespace).Watch(ctx, metav1.ListOptions{
				FieldSelector:   "metadata.name=" + w.configMapName,
				ResourceVersion: w.lastResourceVersion,
			})
			if err != nil {
				logger.Error().Err(err).Msg("Failed to create ConfigMap watcher")
				time.Sleep(10 * time.Second)
				continue
			}
			for event := range watcher.ResultChan() {
				select {
				case <-ctx.Done():
					watcher.Stop()
					logger.Info().Msg("Config watcher shutting down")
					return
				default:
				}
				if event.Type == watch.Error {
					logger.Error().Msg("Error watching ConfigMap")
					break
				}
				configMap, ok := event.Object.(*v1.ConfigMap)
				if !ok {
					logger.Warn().Msg("Unexpected object type in ConfigMap watcher")
					continue
				}
				w.lastResourceVersion = configMap.ResourceVersion
				if event.Type == watch.Added || event.Type == watch.Modified {
					caddyfileData, exists := configMap.Data["Caddyfile"]
					if exists {
						logger.Info().Str("event", string(event.Type)).Msg("ConfigMap updated, notifying handlers")
						w.configHandler(caddyfileData)
					} else {
						logger.Warn().Msg("Updated ConfigMap does not contain Caddyfile key")
					}
				}
			}
			logger.Info().Msg("ConfigMap watcher channel closed, restarting")
			time.Sleep(5 * time.Second)
		}
	}
}
