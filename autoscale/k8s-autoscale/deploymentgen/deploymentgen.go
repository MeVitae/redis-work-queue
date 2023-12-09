package deploymentgen

import (
	"io"
	"maps"

	corev1 "k8s.io/api/core/v1"
	apps "k8s.io/client-go/applyconfigurations/apps/v1"
	core "k8s.io/client-go/applyconfigurations/core/v1"
	meta "k8s.io/client-go/applyconfigurations/meta/v1"
	"sigs.k8s.io/yaml"
)

func ptr[T any](value T) *T {
	return &value
}

type Deployment struct {
	// Namespace of the Kubernetes deployment
	Namespace string

	// Name of the Kubernetes deployment
	Name string

	// PodName is the name of the pods created by the deployment, and the value of the "app" label.
	PodName string `yaml:"podName"`

	// Image is the container image to use.
	Image string

	// DefaultScale is the default number of replicas.
	DefaultScale int32 `yaml:"defaultScale"`

	// RevisionHistoryLimit is the number of past deployment revisions kept around.
	RevisionHistoryLimit int32 `yaml:"revisionHistoryLimit"`

	// Resources is the resource requests and limits for the pods.
	Resources *core.ResourceRequirementsApplyConfiguration

	// Volumes to apply to the deployment.
	Volumes []core.VolumeApplyConfiguration

	// VolumeMounts for the generated containers.
	VolumeMounts []core.VolumeMountApplyConfiguration `yaml:"volumeMounts"`

	// Env config for the pods.
	Env []core.EnvVarApplyConfiguration

	// NodeSelector to apply to the pods.
	NodeSelector map[string]string

	// Spot determines if the worker should only run on spot machines.
	//
	// This is currently only supported on GKE, though support for AKS is planned.
	//
	// Currently, it adds the "cloud.google.com/gke-spot" node selector toleration to the pods.
	Spot bool

	// AddSpot, if true, causes a new spot deployment to be automatically created.
	//
	// The new deployment will have "-spot" appended to its Name and PodName, it will have its
	// DefaultScale set to 0, and Spot set to true.
	AddSpot bool `yaml:"addSpot"`
}

// Generate the Kubernetes deployment from the deployment description.
func (deployment *Deployment) Generate() *apps.DeploymentApplyConfiguration {
	deploymentConfig := &apps.DeploymentApplyConfiguration{
		TypeMetaApplyConfiguration: meta.TypeMetaApplyConfiguration{
			Kind:       ptr("Deployment"),
			APIVersion: ptr("apps/v1"),
		},
		ObjectMetaApplyConfiguration: &meta.ObjectMetaApplyConfiguration{
			Name:      &deployment.Name,
			Namespace: &deployment.Namespace,
			Labels: map[string]string{
				"app": deployment.PodName,
			},
		},
		Spec: &apps.DeploymentSpecApplyConfiguration{
			Replicas:             &deployment.DefaultScale,
			RevisionHistoryLimit: &deployment.RevisionHistoryLimit,
			Selector: &meta.LabelSelectorApplyConfiguration{
				MatchLabels: map[string]string{
					"app": deployment.PodName,
				},
			},
			Template: &core.PodTemplateSpecApplyConfiguration{
				ObjectMetaApplyConfiguration: &meta.ObjectMetaApplyConfiguration{
					Labels: map[string]string{
						"app": deployment.PodName,
					},
				},
				Spec: &core.PodSpecApplyConfiguration{
					Containers: []core.ContainerApplyConfiguration{{
						Name:         &deployment.PodName,
						Image:        &deployment.Image,
						Resources:    deployment.Resources,
						VolumeMounts: deployment.VolumeMounts,
						Env:          deployment.Env,
					}},
					RestartPolicy: ptr(corev1.RestartPolicyAlways),
					Volumes:       deployment.Volumes,
				},
			},
		},
	}
	if deployment.Spot {
		var nodeSelector map[string]string
		if deployment.NodeSelector != nil {
			nodeSelector = maps.Clone(deployment.NodeSelector)
		} else {
			nodeSelector = make(map[string]string)
		}
		nodeSelector["cloud.google.com/gke-spot"] = "true"
		deploymentConfig.Spec.Template.Spec.NodeSelector = nodeSelector
		deploymentConfig.Spec.Template.Spec.Tolerations = []core.TolerationApplyConfiguration{{
			Key:      ptr("cloud.google.com/gke-spot"),
			Operator: ptr(corev1.TolerationOpEqual),
			Value:    ptr("true"),
			Effect:   ptr(corev1.TaintEffectNoSchedule),
		}}
	} else {
		deploymentConfig.Spec.Template.Spec.NodeSelector = deployment.NodeSelector
	}
	return deploymentConfig
}

// GenerateAll the Kubernetes deployments from a list of deployment descriptions.
//
// You should use AddSpot before calling this.
func GenerateAll(deployments []Deployment) []*apps.DeploymentApplyConfiguration {
	k8sDeployments := make([]*apps.DeploymentApplyConfiguration, 0, len(deployments))
	for _, deployment := range deployments {
		if deployment.AddSpot {
			panic("AddSpot was not called before GenerateAll")
		}
		k8sDeployments = append(k8sDeployments, deployment.Generate())
	}
	return k8sDeployments
}

// AddSpot adds spot workers for all workers in deployments with AddSpot set to true.
//
// This sets AddSpot to false for the deployments processed copied.
func AddSpot(deployments *[]Deployment) {
	count := len(*deployments)
	for idx := 0; idx < count; idx++ {
		deployment := &(*deployments)[idx]
		if deployment.AddSpot {
			// Remove AddSpot since we're adding it now
			deployment.AddSpot = false
			if deployment.Spot {
				panic("cannot add spot to spot deployment")
			}
			spotDeployment := *deployment
			spotDeployment.Spot = true
			spotDeployment.DefaultScale = 0
			spotDeployment.Name += "-spot"
			spotDeployment.PodName += "-spot"
			*deployments = append(*deployments, spotDeployment)
		}
	}
}

// WriteMultipleYaml writes multiple YAML items, separeted by "---", to w.
func WriteMultipleYaml[T any](w io.Writer, items []T) error {
	for idx, item := range items {
		if idx != 0 {
			_, err := w.Write([]byte("\n---\n"))
			if err != nil {
				return err
			}
		}
		data, err := yaml.Marshal(item)
		if err != nil {
			return err
		}
		_, err = w.Write(data)
		if err != nil {
			return err
		}
	}
	return nil
}
