package watcher

import (
	"context"
	"time"

	"github.com/rs/zerolog/log"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"

	"gl.vprw.ru/vapronva/ckic/pkg/constants"
)

type ConfigHandlerFunc func(string)

type ConfigWatcher struct {
	clientset           *kubernetes.Clientset
	namespace           string
	configMapName       string
	configHandler       ConfigHandlerFunc
	lastResourceVersion string
}

func NewConfigWatcher(clientset *kubernetes.Clientset, namespace, configMapName string, handler ConfigHandlerFunc) *ConfigWatcher {
	return &ConfigWatcher{
		clientset:     clientset,
		namespace:     namespace,
		configMapName: configMapName,
		configHandler: handler,
	}
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
		logger.Error().Err(err).Msg("Failed to get initial ConfigMap, creating a new one")
		configMap = &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      w.configMapName,
				Namespace: w.namespace,
			},
			Data: map[string]string{
				"Caddyfile": ":80 {\n    respond \"Hello, world!\"\n}\n",
			},
		}
		configMap, err = w.clientset.CoreV1().ConfigMaps(w.namespace).Create(ctx, configMap, metav1.CreateOptions{})
		if err != nil {
			logger.Error().Err(err).Msg("Failed to create default ConfigMap")
		}
	} else {
		if caddyfileData, exists := configMap.Data["Caddyfile"]; exists {
			logger.Info().Msg("Initial ConfigMap loaded, notifying handlers")
			w.configHandler(caddyfileData)
		} else {
			logger.Warn().Msg("ConfigMap does not contain Caddyfile key")
		}
	}
	w.lastResourceVersion = configMap.ResourceVersion
	delay := constants.ConfigMapWatcherInitialDelay
	maxDelay := constants.ConfigMapWatcherMaxDelay
	multiplier := 1.5
	for {
		select {
		case <-ctx.Done():
			logger.Info().Msg("Config watcher shutting down")
			return
		default:
		}
		watcher, err := w.clientset.CoreV1().ConfigMaps(w.namespace).Watch(ctx, metav1.ListOptions{
			FieldSelector:   "metadata.name=" + w.configMapName,
			ResourceVersion: w.lastResourceVersion,
		})
		if err != nil {
			logger.Error().Err(err).Msg("Failed to create ConfigMap watcher, retrying")
			w.lastResourceVersion = ""
			time.Sleep(delay)
			delay = min(time.Duration(float64(delay)*multiplier), maxDelay)
			continue
		}
		delay = constants.ConfigMapWatcherInitialDelay
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
			cm, ok := event.Object.(*corev1.ConfigMap)
			if !ok {
				logger.Warn().Msg("Unexpected object type in ConfigMap watcher")
				continue
			}
			w.lastResourceVersion = cm.ResourceVersion
			if event.Type == watch.Added || event.Type == watch.Modified {
				if caddyfileData, exists := cm.Data["Caddyfile"]; exists {
					logger.Info().Str("event", string(event.Type)).Msg("ConfigMap updated, notifying handlers")
					w.configHandler(caddyfileData)
				} else {
					logger.Warn().Msg("Updated ConfigMap does not contain Caddyfile key")
				}
			}
		}
		logger.Info().Msg("ConfigMap watcher channel closed, restarting")
		time.Sleep(delay)
		delay = min(time.Duration(float64(delay)*multiplier), maxDelay)
	}
}
