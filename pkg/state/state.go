package state

import (
	"context"
	"encoding/json"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"gl.vprw.ru/vapronva/ckic/pkg/caddy"
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

func NewConfigMapStateStore(client kubernetes.Interface, namespace, name string) *ConfigMapStateStore {
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
	cm, err := s.Client.CoreV1().ConfigMaps(s.Namespace).Get(
		context.Background(), s.Name, metav1.GetOptions{})
	if err == nil {
		if cm.Data == nil {
			cm.Data = make(map[string]string)
		}
		cm.Data["state"] = string(data)
		_, err = s.Client.CoreV1().ConfigMaps(s.Namespace).Update(
			context.Background(), cm, metav1.UpdateOptions{})
		if err != nil {
			return fmt.Errorf("failed to update state ConfigMap: %w", err)
		}
	} else {
		cm = &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      s.Name,
				Namespace: s.Namespace,
			},
			Data: map[string]string{
				"state": string(data),
			},
		}
		_, err = s.Client.CoreV1().ConfigMaps(s.Namespace).Create(
			context.Background(), cm, metav1.CreateOptions{})
		if err != nil {
			return fmt.Errorf("failed to create state ConfigMap: %w", err)
		}
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
	data, exists := cm.Data["state"]
	if !exists {
		return stateMap, nil
	}
	err = json.Unmarshal([]byte(data), &stateMap)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal state: %w", err)
	}
	return stateMap, nil
}
