package main

import (
	"fmt"
	"os"
	"path"

	"sigs.k8s.io/yaml"

	corev1 "k8s.io/api/core/v1"
	apps "k8s.io/client-go/applyconfigurations/apps/v1"
	core "k8s.io/client-go/applyconfigurations/core/v1"
	meta "k8s.io/client-go/applyconfigurations/meta/v1"
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
					}},
					RestartPolicy: ptr(corev1.RestartPolicyAlways),
					Volumes:       deployment.Volumes,
				},
			},
		},
	}
	if deployment.Spot {
		deploymentConfig.Spec.Template.Spec.NodeSelector = map[string]string{
			"cloud.google.com/gke-spot": "true",
		}
		deploymentConfig.Spec.Template.Spec.Tolerations = []core.TolerationApplyConfiguration{{
			Key:      ptr("cloud.google.com/gke-spot"),
			Operator: ptr(corev1.TolerationOpEqual),
			Value:    ptr("true"),
			Effect:   ptr(corev1.TaintEffectNoSchedule),
		}}
	}
	return deploymentConfig
}

// AddSpot adds spot workers for all workers in deployments with AddSpot set to true.
func AddSpot(deployments []Deployment) []Deployment {
	count := len(deployments)
	for idx := 0; idx < count; idx++ {
		deployment := &deployments[idx]
		if deployment.AddSpot {
			if deployment.Spot {
				panic("cannot add spot to spot deployment")
			}
			spotDeployment := *deployment
			spotDeployment.Spot = true
			spotDeployment.DefaultScale = 0
			spotDeployment.Name += "-spot"
			spotDeployment.PodName += "-spot"
			deployments = append(deployments, spotDeployment)
		}
	}
	return deployments
}

func main() {
	if len(os.Args) != 2 || os.Args[1] == "-h" || os.Args[1] == "--help" {
		bin := path.Base(os.Args[0])
		fmt.Fprint(os.Stderr, "Usage: ", bin, " deployments.yaml\n\n")
		fmt.Fprint(os.Stderr, "Output the Kubernetes deployments to stdout\n\n")
		fmt.Fprint(os.Stderr, "Example deployment: ", bin, " deployments.yaml | kubectl apply -f\n\n")
		fmt.Fprintln(os.Stderr, "The deployment.yaml file should contain a list of Deployment entries:")
		fmt.Fprintln(os.Stderr, `
    namespace: string, the namespace of the Kubernetes deployment

    name: string, of the Kubernetes deployment

    podName: string, the name of the pods created by the deployment, and the value of the "app" label.

    image: string, the container image to use.

    defaultScale: int32, the default number of replicas.

    revisionHistoryLimit: int32, the number of past deployment revisions kept around.

    resources: the resource requests and limits for the pods,
               see https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/

    spot: bool, determines if the worker should only run on spot machines.
                This is currently only supported on GKE, though support for AKS is planned.
                Currently, it adds the "cloud.google.com/gke-spot" node selector toleration to the pods.

    addSpot: bool, if true, causes a new spot deployment to be automatically created.
                   The new deployment will have "-spot" appended to its Name and PodName, it will
                   have its DefaultScale set to 0, and Spot set to true.`)
		if len(os.Args) != 2 || os.Args[1] != "-h" && os.Args[1] != "--help" {
			os.Exit(1)
		}
		return
	}

	// Parse the deployments
	filePath := os.Args[1]
	deploymentsYaml, err := os.ReadFile(filePath)
	if err != nil {
		panic(fmt.Errorf("Failed to read %s: %w", filePath, err))
	}
	var deployments []Deployment
	yaml.Unmarshal(deploymentsYaml, &deployments)
	if err != nil {
		panic(fmt.Errorf("Failed to parse %s: %w", filePath, err))
	}

	// Add spot workers if requested
	deployments = AddSpot(deployments)

	// Generate the Kubernetes deployments!
	for idx, deployment := range deployments {
		if idx != 0 {
			fmt.Println("---")
		}
		spec := deployment.Generate()
		data, err := yaml.Marshal(spec)
		if err != nil {
			panic(err)
		}
		os.Stdout.Write(data)
	}
}
