// package scale implements Deployments and Deployment interface for a Kubernetes cluster.
package scale

import (
	"context"

	autoscaling "k8s.io/api/autoscaling/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	apps "k8s.io/client-go/kubernetes/typed/apps/v1"
	"k8s.io/client-go/rest"

	"github.com/mevitae/redis-work-queue/autoscale/interfaces"
)

// Deployments within a Kubernetes cluster, implementing interfaces.Deployments.
type Deployments struct {
	apps.DeploymentInterface
}

// NewDeploymentsFromConfig gets the deployment from a Kubernetes clientset.
func NewDeployments(clientset *kubernetes.Clientset, namespace string) Deployments {
	return Deployments{
		DeploymentInterface: clientset.AppsV1().Deployments(namespace),
	}
}

// NewDeploymentsFromConfig gets the deployment from a config.
func NewDeploymentsFromConfig(config *rest.Config, namespace string) (Deployments, error) {
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return Deployments{}, err
	}
	return NewDeployments(clientset, namespace), nil
}

// InClusterDeployments returns the deployments from the cluster the process is running within.
func InClusterDeployments(namespace string) (Deployments, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return Deployments{}, err
	}
	return NewDeploymentsFromConfig(config, namespace)
}

func (deployments Deployments) GetDeployment(_ctx context.Context, name string) (interfaces.Deployment, error) {
	return Deployment{
		deployments: deployments,
		name:        name,
	}, nil
}

// Deployment within a Kubernetes cluster, implementing interfaces.Deployment.
type Deployment struct {
	deployments Deployments
	name        string
}

func (deployment Deployment) GetReady(ctx context.Context) (int32, error) {
	deploy, err := deployment.deployments.Get(ctx, deployment.name, meta.GetOptions{})
	if err != nil {
		return 0, err
	}
	return deploy.Status.ReadyReplicas, nil
}

func (deployment Deployment) GetRequest(ctx context.Context) (int32, error) {
	scale, err := deployment.deployments.GetScale(ctx, deployment.name, meta.GetOptions{})
	if err != nil {
		return 0, err
	}
	return scale.Spec.Replicas, nil
}

func (deployment Deployment) SetRequest(ctx context.Context, count int32) error {
	var err error
	for attempt := 0; attempt < 8; attempt++ {
		// Load the current scale
		var scale *autoscaling.Scale
		scale, err = deployment.deployments.GetScale(ctx, deployment.name, meta.GetOptions{})
		if err != nil {
			return err
		}
		// Update the number of replicas
		scale.Spec.Replicas = count
		// Attempt to update it, retrying upto 8 times if it fails
		_, err = deployment.deployments.UpdateScale(ctx, deployment.name, scale, meta.UpdateOptions{})
		if err == nil {
			return nil
		}
	}
	return err
}
