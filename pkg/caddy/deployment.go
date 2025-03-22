package caddy

import (
	"context"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
)

type Instance struct {
	NodeName       string
	Namespace      string
	PodName        string
	ServiceName    string
	DeploymentName string
	FailureCount   int
	KubeClient     *kubernetes.Clientset
}

func DeployCaddy(clientset *kubernetes.Clientset, nodeName, namespace, caddyImage string, enableNodePort bool) (*Instance, error) {
	ctx := context.Background()
	logger := log.With().Str("node", nodeName).Logger()
	deploymentName := fmt.Sprintf("caddy-%s", nodeName)
	instance := &Instance{
		NodeName:       nodeName,
		Namespace:      namespace,
		DeploymentName: deploymentName,
		ServiceName:    deploymentName,
		FailureCount:   0,
		KubeClient:     clientset,
	}
	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      deploymentName,
			Namespace: namespace,
			Labels: map[string]string{
				"app":                        "caddy",
				"instance":                   nodeName,
				"ckic.cmld.ru/caddy-managed": "true",
			},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: int32Ptr(1),
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app":      "caddy",
					"instance": nodeName,
				},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app":                        "caddy",
						"instance":                   nodeName,
						"ckic.cmld.ru/caddy-managed": "true",
					},
				},
				Spec: corev1.PodSpec{
					NodeSelector: map[string]string{
						"kubernetes.io/hostname": nodeName,
					},
					Containers: []corev1.Container{
						{
							Name:  "caddy",
							Image: caddyImage,
							Ports: []corev1.ContainerPort{
								{
									Name:          "http",
									ContainerPort: 80,
									Protocol:      corev1.ProtocolTCP,
								},
								{
									Name:          "https",
									ContainerPort: 443,
									Protocol:      corev1.ProtocolTCP,
								},
								{
									Name:          "admin",
									ContainerPort: 2019,
									Protocol:      corev1.ProtocolTCP,
								},
							},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "caddy-config",
									MountPath: "/etc/caddy/Caddyfile",
									SubPath:   "Caddyfile",
								},
								{
									Name:      "opt-data",
									MountPath: "/data",
								},
								{
									Name:      "opt-config",
									MountPath: "/config",
								},
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "caddy-config",
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{
										Name: "caddy-default-config",
									},
									Items: []corev1.KeyToPath{
										{
											Key:  "Caddyfile",
											Path: "Caddyfile",
										},
									},
								},
							},
						},
						{
							Name: "opt-data",
							VolumeSource: corev1.VolumeSource{
								HostPath: &corev1.HostPathVolumeSource{
									Path: "/opt/cmld/caddy/data",
									Type: hostPathTypePtr(corev1.HostPathDirectoryOrCreate),
								},
							},
						},
						{
							Name: "opt-config",
							VolumeSource: corev1.VolumeSource{
								HostPath: &corev1.HostPathVolumeSource{
									Path: "/opt/cmld/caddy/config",
									Type: hostPathTypePtr(corev1.HostPathDirectoryOrCreate),
								},
							},
						},
					},
				},
			},
		},
	}
	existingDeployment, err := clientset.AppsV1().Deployments(namespace).Get(ctx, deploymentName, metav1.GetOptions{})
	if err == nil {
		deployment.ResourceVersion = existingDeployment.ResourceVersion
		_, err = clientset.AppsV1().Deployments(namespace).Update(ctx, deployment, metav1.UpdateOptions{})
		if err != nil {
			logger.Error().Err(err).Msg("Failed to update existing Caddy deployment")
			return nil, fmt.Errorf("failed to update deployment %s: %w", deploymentName, err)
		}
		logger.Info().Msg("Updated existing Caddy deployment")
	} else {
		_, err = clientset.AppsV1().Deployments(namespace).Create(ctx, deployment, metav1.CreateOptions{})
		if err != nil {
			logger.Error().Err(err).Msg("Failed to create Caddy deployment")
			return nil, fmt.Errorf("failed to create deployment %s: %w", deploymentName, err)
		}
		logger.Info().Msg("Created Caddy deployment")
	}
	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      deploymentName,
			Namespace: namespace,
			Labels: map[string]string{
				"app":                        "caddy",
				"instance":                   nodeName,
				"ckic.cmld.ru/caddy-managed": "true",
			},
		},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{
				"app":      "caddy",
				"instance": nodeName,
			},
			Ports: []corev1.ServicePort{
				{
					Name:       "admin",
					Port:       2019,
					TargetPort: intstr.FromInt(2019),
					Protocol:   corev1.ProtocolTCP,
				},
				{
					Name:       "http",
					Port:       80,
					TargetPort: intstr.FromInt(80),
					Protocol:   corev1.ProtocolTCP,
				},
				{
					Name:       "https",
					Port:       443,
					TargetPort: intstr.FromInt(443),
					Protocol:   corev1.ProtocolTCP,
				},
			},
			Type: corev1.ServiceTypeClusterIP,
		},
	}
	existingService, err := clientset.CoreV1().Services(namespace).Get(ctx, deploymentName, metav1.GetOptions{})
	if err == nil {
		service.ResourceVersion = existingService.ResourceVersion
		_, err = clientset.CoreV1().Services(namespace).Update(ctx, service, metav1.UpdateOptions{})
		if err != nil {
			logger.Error().Err(err).Msg("Failed to update existing Caddy service")
			return nil, fmt.Errorf("failed to update service %s: %w", deploymentName, err)
		}
		logger.Info().Msg("Updated existing Caddy service")
	} else {
		_, err = clientset.CoreV1().Services(namespace).Create(ctx, service, metav1.CreateOptions{})
		if err != nil {
			logger.Error().Err(err).Msg("Failed to create Caddy service")
			return nil, fmt.Errorf("failed to create service %s: %w", deploymentName, err)
		}
		logger.Info().Msg("Created Caddy service")
	}
	if enableNodePort {
		nodePortServiceName := deploymentName + "-nodeport"
		nodePortService := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:      nodePortServiceName,
				Namespace: namespace,
				Labels: map[string]string{
					"app":                        "caddy",
					"instance":                   nodeName,
					"ckic.cmld.ru/caddy-managed": "true",
				},
			},
			Spec: corev1.ServiceSpec{
				Selector: map[string]string{
					"app":      "caddy",
					"instance": nodeName,
				},
				Ports: []corev1.ServicePort{
					{
						Name:       "http-tcp",
						Port:       80,
						TargetPort: intstr.FromInt(80),
						Protocol:   corev1.ProtocolTCP,
					},
					{
						Name:       "http-udp",
						Port:       80,
						TargetPort: intstr.FromInt(80),
						Protocol:   corev1.ProtocolUDP,
					},
					{
						Name:       "https-tcp",
						Port:       443,
						TargetPort: intstr.FromInt(443),
						Protocol:   corev1.ProtocolTCP,
					},
					{
						Name:       "https-udp",
						Port:       443,
						TargetPort: intstr.FromInt(443),
						Protocol:   corev1.ProtocolUDP,
					},
				},
				Type: corev1.ServiceTypeNodePort,
			},
		}
		existingNPService, errNPService := clientset.CoreV1().Services(namespace).Get(ctx, nodePortServiceName, metav1.GetOptions{})
		if errNPService == nil {
			nodePortService.ResourceVersion = existingNPService.ResourceVersion
			_, err = clientset.CoreV1().Services(namespace).Update(ctx, nodePortService, metav1.UpdateOptions{})
			if err != nil {
				logger.Error().Err(err).Msg("Failed to update existing NodePort service")
				return nil, fmt.Errorf("failed to update NodePort service %s: %w", nodePortServiceName, err)
			}
			logger.Info().Msg("Updated existing NodePort service")
		} else {
			_, err = clientset.CoreV1().Services(namespace).Create(ctx, nodePortService, metav1.CreateOptions{})
			if err != nil {
				logger.Error().Err(err).Msg("Failed to create NodePort service")
				return nil, fmt.Errorf("failed to create NodePort service %s: %w", nodePortServiceName, err)
			}
			logger.Info().Msg("Created NodePort service")
		}
	}
	err = waitForDeploymentReady(clientset, namespace, deploymentName)
	if err != nil {
		logger.Error().Err(err).Msg("Deployment failed to become ready")
		clientset.CoreV1().Services(namespace).Delete(ctx, deploymentName, metav1.DeleteOptions{})
		clientset.AppsV1().Deployments(namespace).Delete(ctx, deploymentName, metav1.DeleteOptions{})
		return nil, fmt.Errorf("deployment %s/%s did not become ready: %w", namespace, deploymentName, err)
	}
	pods, err := clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("app=caddy,instance=%s", nodeName),
	})
	if err != nil || len(pods.Items) == 0 {
		logger.Error().Err(err).Msg("Failed to get Caddy pod name")
		return instance, nil
	}
	instance.PodName = pods.Items[0].Name
	logger.Info().Str("pod", instance.PodName).Msg("Caddy instance deployed successfully")
	return instance, nil
}

func (i *Instance) Delete(clientset *kubernetes.Clientset) error {
	ctx := context.Background()
	logger := log.With().
		Str("node", i.NodeName).
		Str("deployment", i.DeploymentName).
		Logger()
	err := clientset.CoreV1().Services(i.Namespace).Delete(ctx, i.ServiceName, metav1.DeleteOptions{})
	if err != nil {
		logger.Warn().Err(err).Msg("Failed to delete ClusterIP Caddy service")
	} else {
		logger.Info().Msg("Deleted ClusterIP Caddy service")
	}
	nodePortServiceName := i.DeploymentName + "-nodeport"
	errNP := clientset.CoreV1().Services(i.Namespace).Delete(ctx, nodePortServiceName, metav1.DeleteOptions{})
	if errNP != nil {
		logger.Warn().Err(errNP).Msg("Failed to delete NodePort Caddy service (if exists)")
	} else {
		logger.Info().Msg("Deleted NodePort Caddy service")
	}
	errDep := clientset.AppsV1().Deployments(i.Namespace).Delete(ctx, i.DeploymentName, metav1.DeleteOptions{})
	if errDep != nil {
		logger.Error().Err(errDep).Msg("Failed to delete Caddy deployment")
		return fmt.Errorf("failed to delete deployment %s: %w", i.DeploymentName, errDep)
	}
	logger.Info().Msg("Deleted Caddy deployment")
	return nil
}

func waitForDeploymentReady(clientset *kubernetes.Clientset, namespace, name string) error {
	ctx := context.Background()
	logger := log.With().
		Str("deployment", name).
		Str("namespace", namespace).
		Logger()
	for range 24 {
		deployment, err := clientset.AppsV1().Deployments(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("failed to get deployment %s: %w", name, err)
		}
		if deployment.Status.ReadyReplicas > 0 {
			logger.Info().Msg("Deployment is ready")
			return nil
		}
		logger.Debug().
			Int32("available", deployment.Status.AvailableReplicas).
			Int32("ready", deployment.Status.ReadyReplicas).
			Int32("desired", *deployment.Spec.Replicas).
			Msg("Waiting for deployment to be ready")
		time.Sleep(5 * time.Second)
	}
	return fmt.Errorf("deployment %s/%s did not become ready within timeout", namespace, name)
}

func int32Ptr(i int32) *int32 {
	return &i
}

func hostPathTypePtr(t corev1.HostPathType) *corev1.HostPathType {
	return &t
}
