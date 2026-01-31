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

func DeployCaddy(
	ctx context.Context,
	clientset *kubernetes.Clientset,
	nodeName,
	namespace,
	caddyImage string,
	enableLoadBalancer bool,
	externalIPs []string,
	envSecretName string,
	envSecretKeys []string,
	dataVolumePVC string,
	configVolumePVC string,
	configMapName string,
	useHostNetwork bool,
	httpHostPort int,
	httpsHostPort int,
) (*Instance, error) {
	deployCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	logger := log.With().Str("node", nodeName).Logger()
	deploymentName := fmt.Sprintf("caddy-%s", nodeName)
	instance := &Instance{
		NodeName:       nodeName,
		Namespace:      namespace,
		DeploymentName: deploymentName,
		ServiceName:    deploymentName,
		ExternalIPs:    externalIPs,
		KubeClient:     clientset,
	}
	if err := deployDeployment(
		deployCtx,
		clientset,
		instance,
		caddyImage,
		envSecretName,
		envSecretKeys,
		dataVolumePVC,
		configVolumePVC,
		configMapName,
		useHostNetwork,
		httpHostPort,
		httpsHostPort,
	); err != nil {
		return nil, err
	}
	if !useHostNetwork {
		if err := deployService(deployCtx, clientset, instance); err != nil {
			return nil, err
		}
		if enableLoadBalancer {
			if err := deployLoadBalancerService(deployCtx, clientset, instance); err != nil {
				return nil, err
			}
		}
	} else {
		logger.Info().Msg("Skipping service creation due to hostNetwork mode")
	}
	if err := waitForDeploymentReady(deployCtx, clientset, namespace, deploymentName); err != nil {
		cleanupDeployment(ctx, clientset, instance)
		return nil, fmt.Errorf("deployment %s/%s did not become ready: %w", namespace, deploymentName, err)
	}
	pods, err := clientset.CoreV1().Pods(namespace).List(deployCtx, metav1.ListOptions{
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

func deployDeployment(
	ctx context.Context,
	clientset *kubernetes.Clientset,
	instance *Instance,
	caddyImage string,
	envSecretName string,
	envSecretKeys []string,
	dataVolumePVC string,
	configVolumePVC string,
	configMapName string,
	useHostNetwork bool,
	httpHostPort int,
	httpsHostPort int,
) error {
	logger := log.With().Str("deployment", instance.DeploymentName).Logger()
	replicas := int32(1)
	caddyContainer := corev1.Container{
		Name:  "caddy",
		Image: caddyImage,
		Ports: []corev1.ContainerPort{
			{Name: "admin", ContainerPort: 2019, Protocol: corev1.ProtocolTCP},
		},
		VolumeMounts: []corev1.VolumeMount{
			{Name: "caddy-config", MountPath: "/etc/caddy/Caddyfile", SubPath: "Caddyfile"},
			{Name: "opt-data", MountPath: "/data"},
			{Name: "opt-config", MountPath: "/config"},
		},
		ImagePullPolicy: corev1.PullPolicy("Always"),
		SecurityContext: &corev1.SecurityContext{
			Capabilities: &corev1.Capabilities{
				Add:  []corev1.Capability{"NET_ADMIN", "NET_BIND_SERVICE"},
				Drop: []corev1.Capability{"ALL"},
			},
		},
	}
	if useHostNetwork {
		logger.Info().Int("httpPort", httpHostPort).Int("httpsPort", httpsHostPort).Msg("Configuring hostNetwork with host ports")
		caddyContainer.Ports = append(caddyContainer.Ports,
			corev1.ContainerPort{
				Name:          "http-tcp",
				ContainerPort: 80,
				Protocol:      corev1.ProtocolTCP,
				HostPort:      int32(httpHostPort),
			},
			corev1.ContainerPort{
				Name:          "http-udp",
				ContainerPort: 80,
				Protocol:      corev1.ProtocolUDP,
				HostPort:      int32(httpHostPort),
			},
			corev1.ContainerPort{
				Name:          "https-tcp",
				ContainerPort: 443,
				Protocol:      corev1.ProtocolTCP,
				HostPort:      int32(httpsHostPort),
			},
			corev1.ContainerPort{
				Name:          "https-udp",
				ContainerPort: 443,
				Protocol:      corev1.ProtocolUDP,
				HostPort:      int32(httpsHostPort),
			},
		)
	} else {
		caddyContainer.Ports = append(caddyContainer.Ports,
			corev1.ContainerPort{Name: "http-tcp", ContainerPort: 80, Protocol: corev1.ProtocolTCP},
			corev1.ContainerPort{Name: "http-udp", ContainerPort: 80, Protocol: corev1.ProtocolUDP},
			corev1.ContainerPort{Name: "https-tcp", ContainerPort: 443, Protocol: corev1.ProtocolTCP},
			corev1.ContainerPort{Name: "https-udp", ContainerPort: 443, Protocol: corev1.ProtocolUDP},
		)
	}
	var envVars []corev1.EnvVar
	if envSecretName != "" && len(envSecretKeys) > 0 {
		logger.Info().Str("secret", envSecretName).Strs("keys", envSecretKeys).Msg("Configuring environment variables from secret")
		for _, key := range envSecretKeys {
			envVars = append(envVars, corev1.EnvVar{
				Name: key,
				ValueFrom: &corev1.EnvVarSource{
					SecretKeyRef: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{
							Name: envSecretName,
						},
						Key: key,
					},
				},
			})
		}
	}
	envVars = append(envVars, corev1.EnvVar{
		Name: "CKIC_POD_NAME",
		ValueFrom: &corev1.EnvVarSource{
			FieldRef: &corev1.ObjectFieldSelector{
				FieldPath: "metadata.name",
			},
		},
	})
	envVars = append(envVars, corev1.EnvVar{
		Name: "CKIC_NODE_NAME",
		ValueFrom: &corev1.EnvVarSource{
			FieldRef: &corev1.ObjectFieldSelector{
				FieldPath: "spec.nodeName",
			},
		},
	})
	envVars = append(envVars, corev1.EnvVar{
		Name: "CKIC_POD_IP",
		ValueFrom: &corev1.EnvVarSource{
			FieldRef: &corev1.ObjectFieldSelector{
				FieldPath: "status.podIP",
			},
		},
	})
	caddyContainer.Env = envVars
	volumes := []corev1.Volume{
		{
			Name: "caddy-config",
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: configMapName,
					},
					Items: []corev1.KeyToPath{
						{Key: "Caddyfile", Path: "Caddyfile"},
					},
				},
			},
		},
	}
	if dataVolumePVC != "" {
		logger.Info().Str("pvc", dataVolumePVC).Msg("Using PVC for data volume")
		volumes = append(volumes, corev1.Volume{
			Name: "opt-data",
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: dataVolumePVC,
				},
			},
		})
	} else {
		logger.Info().Msg("Using HostPath for data volume")
		volumes = append(volumes, corev1.Volume{
			Name: "opt-data",
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: "/opt/cmld/caddy/data",
					Type: hostPathTypePtr(corev1.HostPathDirectoryOrCreate),
				},
			},
		})
	}
	if configVolumePVC != "" {
		logger.Info().Str("pvc", configVolumePVC).Msg("Using PVC for config volume")
		volumes = append(volumes, corev1.Volume{
			Name: "opt-config",
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: configVolumePVC,
				},
			},
		})
	} else {
		logger.Info().Msg("Using HostPath for config volume")
		volumes = append(volumes, corev1.Volume{
			Name: "opt-config",
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: "/opt/cmld/caddy/config",
					Type: hostPathTypePtr(corev1.HostPathDirectoryOrCreate),
				},
			},
		})
	}
	podSpec := corev1.PodSpec{
		NodeSelector: map[string]string{
			"kubernetes.io/hostname": instance.NodeName,
		},
		Containers: []corev1.Container{caddyContainer},
		Volumes:    volumes,
	}
	if useHostNetwork {
		podSpec.HostNetwork = true
		podSpec.DNSPolicy = corev1.DNSClusterFirstWithHostNet
		logger.Info().Msg("Enabled hostNetwork mode")
	}
	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      instance.DeploymentName,
			Namespace: instance.Namespace,
			Labels: map[string]string{
				"app":                        "caddy",
				"instance":                   instance.NodeName,
				"ckic.cmld.ru/caddy-managed": "true",
			},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app":      "caddy",
					"instance": instance.NodeName,
				},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app":                        "caddy",
						"instance":                   instance.NodeName,
						"ckic.cmld.ru/caddy-managed": "true",
					},
				},
				Spec: podSpec,
			},
		},
	}
	existingDeployment, err := clientset.AppsV1().Deployments(instance.Namespace).Get(ctx, instance.DeploymentName, metav1.GetOptions{})
	if err == nil {
		deployment.ResourceVersion = existingDeployment.ResourceVersion
		_, err = clientset.AppsV1().Deployments(instance.Namespace).Update(ctx, deployment, metav1.UpdateOptions{})
		if err != nil {
			logger.Error().Err(err).Msg("Failed to update existing Caddy deployment")
			return fmt.Errorf("failed to update deployment %s: %w", instance.DeploymentName, err)
		}
		logger.Info().Msg("Updated existing Caddy deployment")
	} else {
		_, err = clientset.AppsV1().Deployments(instance.Namespace).Create(ctx, deployment, metav1.CreateOptions{})
		if err != nil {
			logger.Error().Err(err).Msg("Failed to create Caddy deployment")
			return fmt.Errorf("failed to create deployment %s: %w", instance.DeploymentName, err)
		}
		logger.Info().Msg("Created Caddy deployment")
	}
	if err := clientset.PolicyV1().PodDisruptionBudgets(instance.Namespace).Delete(ctx, instance.DeploymentName, metav1.DeleteOptions{}); err == nil {
		logger.Info().Msg("Deleted legacy PodDisruptionBudget")
	}
	return nil
}

func deployService(ctx context.Context, clientset *kubernetes.Clientset, instance *Instance) error {
	logger := log.With().Str("service", instance.ServiceName).Logger()
	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      instance.DeploymentName,
			Namespace: instance.Namespace,
			Labels: map[string]string{
				"app":                        "caddy",
				"instance":                   instance.NodeName,
				"ckic.cmld.ru/caddy-managed": "true",
			},
		},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{
				"app":      "caddy",
				"instance": instance.NodeName,
			},
			Ports: []corev1.ServicePort{
				{
					Name:       "admin",
					Port:       2019,
					TargetPort: intstr.FromInt(2019),
					Protocol:   corev1.ProtocolTCP,
				},
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
			Type: corev1.ServiceTypeClusterIP,
		},
	}
	existingService, err := clientset.CoreV1().Services(instance.Namespace).Get(ctx, instance.DeploymentName, metav1.GetOptions{})
	if err == nil {
		service.ResourceVersion = existingService.ResourceVersion
		_, err = clientset.CoreV1().Services(instance.Namespace).Update(ctx, service, metav1.UpdateOptions{})
		if err != nil {
			logger.Error().Err(err).Msg("Failed to update existing Caddy service")
			return fmt.Errorf("failed to update service %s: %w", instance.DeploymentName, err)
		}
		logger.Info().Msg("Updated existing Caddy service")
	} else {
		_, err = clientset.CoreV1().Services(instance.Namespace).Create(ctx, service, metav1.CreateOptions{})
		if err != nil {
			logger.Error().Err(err).Msg("Failed to create Caddy service")
			return fmt.Errorf("failed to create service %s: %w", instance.DeploymentName, err)
		}
		logger.Info().Msg("Created Caddy service")
	}
	return nil
}

func deployLoadBalancerService(ctx context.Context, clientset *kubernetes.Clientset, instance *Instance) error {
	logger := log.With().Str("loadbalancer_service", instance.DeploymentName+"-loadbalancer").Logger()
	loadBalancerServiceName := instance.DeploymentName + "-loadbalancer"
	loadBalancerService := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      loadBalancerServiceName,
			Namespace: instance.Namespace,
			Labels: map[string]string{
				"app":                        "caddy",
				"instance":                   instance.NodeName,
				"ckic.cmld.ru/caddy-managed": "true",
			},
			Annotations: map[string]string{
				"io.cilium.nodeipam/match-node-labels": "kubernetes.io/hostname=" + instance.NodeName,
			},
		},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{
				"app":      "caddy",
				"instance": instance.NodeName,
			},
			Ports: []corev1.ServicePort{
				{
					Name:       "http-tcp",
					Port:       80,
					TargetPort: intstr.FromInt(80),
					Protocol:   corev1.ProtocolTCP,
				},
				{
					Name:       "https-tcp",
					Port:       443,
					TargetPort: intstr.FromInt(443),
					Protocol:   corev1.ProtocolTCP,
				},
				{
					Name:       "http-udp",
					Port:       80,
					TargetPort: intstr.FromInt(80),
					Protocol:   corev1.ProtocolUDP,
				},
				{
					Name:       "https-udp",
					Port:       443,
					TargetPort: intstr.FromInt(443),
					Protocol:   corev1.ProtocolUDP,
				},
			},
			Type:                  corev1.ServiceTypeLoadBalancer,
			LoadBalancerClass:     StringPtr("io.cilium/node"),
			ExternalTrafficPolicy: corev1.ServiceExternalTrafficPolicyTypeLocal,
		},
	}
	if len(instance.ExternalIPs) > 0 {
		logger.Info().Strs("externalIPs", instance.ExternalIPs).Msg("Setting external IPs for LoadBalancer service")
		loadBalancerService.Spec.ExternalIPs = instance.ExternalIPs
	}
	existingNPService, err := clientset.CoreV1().Services(instance.Namespace).Get(ctx, loadBalancerServiceName, metav1.GetOptions{})
	if err == nil {
		loadBalancerService.ResourceVersion = existingNPService.ResourceVersion
		_, err = clientset.CoreV1().Services(instance.Namespace).Update(ctx, loadBalancerService, metav1.UpdateOptions{})
		if err != nil {
			logger.Error().Err(err).Msg("Failed to update existing LoadBalancer service")
			return fmt.Errorf("failed to update LoadBalancer service %s: %w", loadBalancerServiceName, err)
		}
		logger.Info().Msg("Updated existing LoadBalancer service")
	} else {
		_, err = clientset.CoreV1().Services(instance.Namespace).Create(ctx, loadBalancerService, metav1.CreateOptions{})
		if err != nil {
			logger.Error().Err(err).Msg("Failed to create LoadBalancer service")
			return fmt.Errorf("failed to create LoadBalancer service %s: %w", loadBalancerServiceName, err)
		}
		logger.Info().Msg("Created LoadBalancer service")
	}
	return nil
}

func waitForDeploymentReady(ctx context.Context, clientset *kubernetes.Clientset, namespace, name string) error {
	logger := log.With().Str("deployment", name).Str("namespace", namespace).Logger()
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("context deadline exceeded while waiting for deployment: %w", ctx.Err())
		case <-ticker.C:
			deployment, err := clientset.AppsV1().Deployments(namespace).Get(ctx, name, metav1.GetOptions{})
			if err != nil {
				return fmt.Errorf("failed to get deployment %s: %w", name, err)
			}
			if deployment.Status.ReadyReplicas > 0 {
				logger.Info().Msg("Deployment is ready")
				return nil
			}
			logger.Debug().Int32("available", deployment.Status.AvailableReplicas).Int32("ready", deployment.Status.ReadyReplicas).Int32("desired", *deployment.Spec.Replicas).Msg("Waiting for deployment to be ready")
		}
	}
}

func cleanupDeployment(ctx context.Context, clientset *kubernetes.Clientset, instance *Instance) {
	log.Warn().Str("deployment", instance.DeploymentName).Msg("Cleaning up failed deployment")
	if err := clientset.CoreV1().Services(instance.Namespace).Delete(ctx, instance.ServiceName, metav1.DeleteOptions{}); err != nil {
		log.Debug().Err(err).Str("service", instance.ServiceName).Msg("Failed to delete service during cleanup")
	}
	if err := clientset.AppsV1().Deployments(instance.Namespace).Delete(ctx, instance.DeploymentName, metav1.DeleteOptions{}); err != nil {
		log.Debug().Err(err).Str("deployment", instance.DeploymentName).Msg("Failed to delete deployment during cleanup")
	}
}

func hostPathTypePtr(t corev1.HostPathType) *corev1.HostPathType {
	return &t
}

func StringPtr(s string) *string {
	return &s
}
