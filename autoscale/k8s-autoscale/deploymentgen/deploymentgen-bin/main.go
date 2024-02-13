package main

import (
	"fmt"
	"os"
	"path"

	"sigs.k8s.io/yaml"

	"github.com/mevitae/redis-work-queue/autoscale/k8s-autoscale/deploymentgen"
)

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
	var deployments []deploymentgen.Deployment
	yaml.Unmarshal(deploymentsYaml, &deployments)
	if err != nil {
		panic(fmt.Errorf("Failed to parse %s: %w", filePath, err))
	}

	// Add spot workers if requested
	deploymentgen.AddSpot(&deployments)

	// Generate the Kubernetes deployments!
	k8sDeployments := deploymentgen.GenerateAll(deployments)

	// Output them
	err = deploymentgen.WriteMultipleYaml(os.Stdout, k8sDeployments)
	if err != nil {
		panic(err)
	}
}
