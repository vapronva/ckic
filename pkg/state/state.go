package state

import (
	"context"
	"encoding/json"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"git.horse/vapronva/ckic/pkg/caddy"
	"git.horse/vapronva/ckic/pkg/constants"
)

type StateStore interface {
	SaveState(state map[string]*caddy.Instance) error
	LoadState() (map[string]*caddy.Instance, error)
}

type ConfigMapStateStore struct {
	Client    kubernetes.Interface
	Namespace string
	Name      string
}

func NewConfigMapStateStore(
	client kubernetes.Interface,
	namespace, name string,
) *ConfigMapStateStore {
	return &ConfigMapStateStore{
		Client:    client,
		Namespace: namespace,
		Name:      name,
	}
}

func (s *ConfigMapStateStore) SaveState(state map[string]*caddy.Instance) error {
	data, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("failed to marshal state: %w", err)
	}
	return s.upsertStateConfigMap(context.Background(), string(data))
}

func (s *ConfigMapStateStore) upsertStateConfigMap(ctx context.Context, data string) error {
	cm, err := s.Client.CoreV1().ConfigMaps(s.Namespace).Get(ctx, s.Name, metav1.GetOptions{})
	if err == nil {
		return s.updateStateConfigMap(ctx, cm, data)
	}
	if !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to get state ConfigMap: %w", err)
	}
	cm = &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      s.Name,
			Namespace: s.Namespace,
		},
		Data: map[string]string{
			constants.StateKey: data,
		},
	}
	if _, err = s.Client.CoreV1().
		ConfigMaps(s.Namespace).
		Create(ctx, cm, metav1.CreateOptions{}); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("failed to create state ConfigMap: %w", err)
		}
		existingCM, getErr := s.Client.CoreV1().
			ConfigMaps(s.Namespace).
			Get(ctx, s.Name, metav1.GetOptions{})
		if getErr != nil {
			return fmt.Errorf("failed to get state ConfigMap after create conflict: %w", getErr)
		}
		return s.updateStateConfigMap(ctx, existingCM, data)
	}
	return nil
}

func (s *ConfigMapStateStore) updateStateConfigMap(
	ctx context.Context,
	cm *corev1.ConfigMap,
	data string,
) error {
	if cm.Data == nil {
		cm.Data = make(map[string]string)
	}
	cm.Data[constants.StateKey] = data
	if _, err := s.Client.CoreV1().
		ConfigMaps(s.Namespace).
		Update(ctx, cm, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("failed to update state ConfigMap: %w", err)
	}
	return nil
}

func (s *ConfigMapStateStore) LoadState() (map[string]*caddy.Instance, error) {
	stateMap := make(map[string]*caddy.Instance)
	cm, err := s.Client.CoreV1().ConfigMaps(s.Namespace).Get(
		context.Background(), s.Name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get state ConfigMap: %w", err)
	}
	data, exists := cm.Data[constants.StateKey]
	if !exists {
		return stateMap, nil
	}
	err = json.Unmarshal([]byte(data), &stateMap)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal state: %w", err)
	}
	for _, instance := range stateMap {
		if clientset, ok := s.Client.(*kubernetes.Clientset); ok {
			instance.KubeClient = clientset
		}
	}
	return stateMap, nil
}
