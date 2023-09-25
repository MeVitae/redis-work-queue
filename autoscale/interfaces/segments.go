package interfaces

import (
	"context"
	"strings"
)

// SegmentedDeployments wraps a generic interfaces.Deployments value and adds "segments".
//
// The name syntax of a segmented deployment is `deploymentName/segment`. Each one of these names is
// counted separately, and the underlying deploymentName is scaled to their sum.
//
// If no slash is provided, the default segment, "", is used.
type SegmentedDeployments struct {
	Deployments Deployments
	// segments maps deployment names to a map of segment names to their count.
	segments map[string]*map[string]int32
}

// NewSegmentedDeployments wraps deployments to add segments.
func NewSegmentedDeployments(deployments Deployments) *SegmentedDeployments {
	return &SegmentedDeployments{
		Deployments: deployments,
		segments:    make(map[string]*map[string]int32),
	}
}

func (deployments *SegmentedDeployments) GetDeployment(ctx context.Context, name string) (Deployment, error) {
	// Split the name into `deploymentName/segment`
	deploymentName := name
	segment := ""
	slashIdx := strings.IndexRune(name, '/')
	if slashIdx >= 0 {
		segment = deploymentName[slashIdx+1:]
		deploymentName = deploymentName[:slashIdx]
	}

	// Ensure the segments entry for this deployment exists in the segments map
	segments, ok := deployments.segments[deploymentName]
	if !ok {
		segs := make(map[string]int32)
		segments = &segs
		deployments.segments[deploymentName] = segments
	}

	// Get the underlying deployment
	deployment, err := deployments.Deployments.GetDeployment(ctx, deploymentName)

	return SegmentedDeployment{
		deployment: deployment,
		segment:    segment,
		segments:   segments,
	}, err
}

// SegmentedDeployment from a SegmentedDeployments, implementing interfaces.Deployment.
type SegmentedDeployment struct {
	deployment Deployment
	segment    string
	segments   *map[string]int32
}

func (segdep *SegmentedDeployment) getProportion() float32 {
	mySegmentRequest := int32(0)
	totalRequest := int32(0)
	for segment, request := range *segdep.segments {
		totalRequest += request
		if segment == segdep.segment {
			mySegmentRequest = request
		}
	}
	// Don't divide by 0!!
	if totalRequest == 0 {
		return 0
	}
	return float32(mySegmentRequest) / float32(totalRequest)
}

// GetReady fetches the number of workers of the underlying deployment, and returns the share that
// this segment is responsible for, by calculating the proportion of the request this segment has
// made.
func (segdep SegmentedDeployment) GetReady(ctx context.Context) (int32, error) {
	totalReady, err := segdep.deployment.GetReady(ctx)
	return int32(float32(totalReady) * segdep.getProportion()), err
}

// GetRequest for a segment of a deployment.
func (segdep SegmentedDeployment) GetRequest(ctx context.Context) (int32, error) {
	totalRequest, err := segdep.deployment.GetRequest(ctx)
	return int32(float32(totalRequest) * segdep.getProportion()), err
}

// SetRequest for a segment of a deployment.
func (segdep SegmentedDeployment) SetRequest(ctx context.Context, count int32) error {
	(*segdep.segments)[segdep.segment] = count
	totalRequest := int32(0)
	for _, request := range *segdep.segments {
		totalRequest += request
	}
	if totalRequest < count {
		panic("total request is less than individual request")
	}
	return segdep.deployment.SetRequest(ctx, totalRequest)
}
