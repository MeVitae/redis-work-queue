package wqautoscale

import (
	"context"
	"fmt"
	"slices"

	"github.com/mevitae/redis-work-queue/autoscale/interfaces"
)

// WorkQueues is an interface used to get WorkQueue objects by name.
type WorkQueues interface {
	GetWorkQueue(name string) WorkQueue
}

// WorkQueue is a generic work queue.
type WorkQueue interface {
	// Counts returns the number of items in the queue, along with the number of items currently
	// being processed. The queueLen doesn't include items being processed.
	Counts(context.Context) (queueLen, processing int32, err error)
}

/// ---------------------- Implementation ----------------------

type AutoScale struct {
	// order to scale jobs in, determined by interpreting the child structure as a DAG.
	order []string
	// jobs is a map from job name to Job value.
	jobs map[string]*job
}

func NewAutoScale(
	ctx context.Context,
	workQueues WorkQueues,
	deployments interfaces.Deployments,
	config Config,
) (*AutoScale, error) {
	jobs := make(map[string]*job, len(config.Jobs))
	for name, jobConfig := range config.Jobs {
		job, err := jobConfig.ToJob(ctx, workQueues, deployments)
		if err != nil {
			return nil, fmt.Errorf("failed to construct job %s: %w", name, err)
		}
		jobs[name] = &job
	}
	order, err := config.GetScaleOrder()
	fmt.Println("Job scaling order is:", order)
	return &AutoScale{
		order: order,
		jobs:  jobs,
	}, err
}

// Scale the workers at the provided time.
func (autoscaler *AutoScale) Scale(ctx context.Context, time int64) error {
	// This will store a map of job names to the total rate of incomings jobs.
	// If this is assumed to be zero, no entry will be in the map.
	incomingRates := make(map[string]float32)
	// It's important to scale in the DAG-derived order, so that we have always computed the
	// incoming rates before scaling.
	for _, jobName := range autoscaler.order {
		fmt.Println("----------------------------------------------------------------------")
		fmt.Println("Scaling job:", jobName)

		// Get the incoming rate if one has already been stored
		incomingRate, ok := incomingRates[jobName]
		if !ok {
			incomingRate = 0
		}

		// Then scale this job
		job := autoscaler.jobs[jobName]
		jobRate, err := job.Scale(ctx, time, incomingRate)
		if err != nil {
			return fmt.Errorf("failed to scale job %s: %w", jobName, err)
		}

		// Finally, for each of its children, add the input rate from this job.
		for childName, count := range job.Children {
			childRate, ok := incomingRates[childName]
			if !ok {
				childRate = 0
			}
			childRate += count * jobRate
			incomingRates[childName] = childRate
		}
	}
	return nil
}

// Job is a job with a work queue and workers
type job struct {
	QueueName string

	// Queue is the queue for the jobs that need completing.
	Queue WorkQueue

	// DeploymentTiers of the workers running this job.
	//
	// Tiers will be scaled in index order.
	DeploymentTiers []deploymentTier

	// RunTime is the average time taken for a job to complete on a single worker.
	RunTime int32

	// Children is a map of other job names to the average number of jobs of that type spawned by
	// one job of this type completing.
	Children map[string]float32
}

// Scale the workers for a job and return the expected rate of job completions.
func (job *job) Scale(ctx context.Context, time int64, incomingRate float32) (float32, error) {
	qlen, processing, err := job.Queue.Counts(ctx)
	qlen += processing
	fmt.Printf("Scaling %s. Queue length %d (including %d being processed)\n", job.QueueName, qlen, processing)
	if err != nil {
		return 0, fmt.Errorf("failed to get queue length: %w", err)
	}
	// Scale each tier one at a time:
	totalReadyWorkers := int32(0)
	totalRequestedWorkers := int32(0)
	for tierIdx, tier := range job.DeploymentTiers {
		readyWorkers, requestedWorkers, err := tier.Scale(
			ctx, time,
			totalReadyWorkers, totalRequestedWorkers,
			job.RunTime, qlen, incomingRate,
		)
		if err != nil {
			return 0, fmt.Errorf("failed to scale tier %v: %w", tierIdx, err)
		}
		totalReadyWorkers += readyWorkers
		totalRequestedWorkers += requestedWorkers
	}
	// Return the expected completion rate
	return (float32(totalReadyWorkers) + float32(totalRequestedWorkers)) / (2 * float32(job.RunTime)), nil
}

// DeploymentTier is a tier of workers for a job.
type deploymentTier struct {
	Deployment interfaces.Deployment
	SlowDown   slowDown
	DeploymentTierConfig
}

func deploymentTierFromConfig(
	ctx context.Context,
	deployments interfaces.Deployments,
	config DeploymentTierConfig,
) (deploymentTier, error) {
	if config.TargetTime == 0 && config.MinScale != config.MaxScale {
		panic("TargetTime cannot be 0 if MinScale != MaxScale")
	}
	deployment, err := deployments.GetDeployment(ctx, config.DeploymentName)
	if err != nil {
		return deploymentTier{}, fmt.Errorf("failed to get deployment %s: %w", config.DeploymentName, err)
	}
	return deploymentTier{
		Deployment:           deployment,
		SlowDown:             NewSlowDown(),
		DeploymentTierConfig: config,
	}, nil
}

// Scale the workers for a deployment tier and return the number of current and requested workers.
func (tier *deploymentTier) Scale(
	ctx context.Context,
	currentTime int64,
	currentBaseWorkers int32,
	requestedBaseWorkers int32,
	jobRunTime int32,
	qlen int32,
	jobRate float32,
) (currentScale, requestedScale int32, err error) {
	currentScale, err = tier.Deployment.GetReady(ctx)
	if err != nil {
		err = fmt.Errorf("failed to get ready count: %w", err)
		return
	}
	fmt.Println("Tier has", currentScale, "active workers")

	// If we have no choice over the number of workers, just set it.
	if tier.MinScale == tier.MaxScale {
		requestedScale = tier.MaxScale
		fmt.Println("Scaling to", requestedScale, "(fixed scale)")
		err = tier.Deployment.SetRequest(ctx, requestedScale)
		if err != nil {
			err = fmt.Errorf("failed to set scale request: %w", err)
		}
		return
	}

	// If we have to decide the scale, we need a runtime!
	if tier.TargetTime == 0 {
		panic("TargetTime cannot be 0 if MinScale != MaxScale")
	}

	// Calculate the ideal scale to complete the current jobs in the target time.
	currentIdealScale := idealScale(tier.TargetTime, jobRunTime, qlen)

	// Calculate the number of expected new jobs after the spinup time
	// FIXME: If qlenAfterTime is 0, then this will over-estimate
	afterSpinupNewJobs := int32(jobRate * float32(tier.SpinupTime))
	afterSpinupQlen := qlenAfterTime(tier.SpinupTime, currentBaseWorkers+currentScale, jobRunTime, qlen) + afterSpinupNewJobs
	// Then the ideal scale to handle those jobs in the target time
	afterSpinupIdealScale := idealScale(tier.TargetTime, jobRunTime, afterSpinupQlen)

	// Take the maximum of both ideal scales as our ideal scale
	idealScale := currentIdealScale
	if afterSpinupIdealScale > idealScale {
		idealScale = afterSpinupIdealScale
	}

	// Ensure we always request at least one worker if there's an element in the queue
	if qlen > 0 && idealScale <= 0 {
		idealScale = 1
	}
	// Remove the base worker count
	idealScale -= requestedBaseWorkers
	if idealScale < 0 {
		idealScale = 0
	}

	// Calculate the actual number of workers to request
	tier.SlowDown.Push(currentTime-tier.SlowdownDuration(jobRunTime, qlen), currentTime, idealScale)
	scale := tier.SlowDown.GetScale()
	if scale < tier.MinScale {
		scale = tier.MinScale
	}
	if tier.MaxScale < scale {
		scale = tier.MaxScale
	}
	fmt.Println("Scaling to", scale)
	err = tier.Deployment.SetRequest(ctx, scale)
	if err != nil {
		err = fmt.Errorf("failed to set scale request: %w", err)
	}
	return currentScale, scale, err
}

// idealScale return the number of workers needed to complete `qlen` jobs (taking `runTime` to
// complete on a single worker) in `targetTime`.
func idealScale(targetTime, runTime, qlen int32) int32 {
	// ceil(qlen * runTime / targetTime)
	// Fun fact, with ceil(a/b) = floor((a-1)/b) + 1
	// There's one caveat: integer division rounds towards 0, so we need a special case for when
	// qlen == 0
	if qlen == 0 {
		return 0
	}
	return (qlen*runTime-1)/targetTime + 1
}

func qlenAfterTime(time, nWorkers, runTime, qlen int32) int32 {
	n := qlen - (time*nWorkers)/runTime
	if n < 0 {
		return 0
	}
	return n
}

// slowDown delays the downscaling of worker counts without slowing down the upscaling.
type slowDown struct {
	requests []scaleRequest
}

// scaleRequest is a request for a scale used within SlowDown.
type scaleRequest struct {
	// time of the request
	time int64
	// scale is the requested number of workers
	scale int32
}

func NewSlowDown() slowDown {
	return slowDown{
		requests: make([]scaleRequest, 0, 8),
	}
}

// Push pushes a request at currentTime, while removing any requests older than oldestTime.
func (slowdown *slowDown) Push(oldestTime, currentTime int64, scale int32) {
	req := scaleRequest{
		time:  currentTime,
		scale: scale,
	}
	// First, try to overwrite an old entry:
	idx := 0
	for idx < len(slowdown.requests) {
		if slowdown.requests[idx].time < oldestTime {
			slowdown.requests[idx] = req
			// After overwiring an old entry, we need to clean up any other old entries:
			for idx < len(slowdown.requests) {
				if slowdown.requests[idx].time < oldestTime {
					slowdown.requests = slices.Delete(slowdown.requests, idx, idx+1)
				}
			}
			return
		}
	}
	// If there were no old entries, just append this one.
	slowdown.requests = append(slowdown.requests, req)
}

// GetScale returns the scale that should be used. This is the highest scale of all the stored
// requests.
func (slowdown *slowDown) GetScale() int32 {
	scale := int32(0)
	for _, req := range slowdown.requests {
		if req.scale > scale {
			scale = req.scale
		}
	}
	return scale
}

/// ---------------------- Configuration ----------------------

type Config struct {
	// Jobs maps a job name to its config.
	Jobs map[string]JobConfig
}

type JobConfig struct {
	// WorkQueueName is the name of the work queue to scale for.
	QueueName string `yaml:"queueName"`
	// DeploymentTiers of the workers running this job.
	DeploymentTiers []DeploymentTierConfig `yaml:"deploymentTiers"`
	// RunTime is the average time taken for a job to complete on a single worker.
	RunTime int32 `yaml:"runTime"`
	// Children is a map of other job names to the average number of jobs of that type spawned by
	// one job of this type.
	Children map[string]float32
}

func (config *JobConfig) ToJob(
	ctx context.Context,
	workQueues WorkQueues,
	deployments interfaces.Deployments,
) (job, error) {
	deploymentTiers := make([]deploymentTier, 0, len(config.DeploymentTiers))
	for deploymentTierIdx, deploymentTierConfig := range config.DeploymentTiers {
		tier, err := deploymentTierFromConfig(ctx, deployments, deploymentTierConfig)
		if err != nil {
			return job{}, fmt.Errorf("failed to construct deployment tier %v: %w", deploymentTierIdx, err)
		}
		deploymentTiers = append(deploymentTiers, tier)
	}
	return job{
		QueueName:       config.QueueName,
		Queue:           workQueues.GetWorkQueue(config.QueueName),
		DeploymentTiers: deploymentTiers,
		RunTime:         config.RunTime,
		Children:        config.Children,
	}, nil
}

func (config *Config) GetScaleOrder() (order []string, err error) {
	// The order will contain the names of the jobs to scale, in the order they should be scaled.
	order = make([]string, 0, len(config.Jobs))

	// This contains a list of the names that haven't yet been added to `order`.
	remainingJobs := make([]string, 0, len(config.Jobs))
	// Initially, this is the name of every job.
	for jobName := range config.Jobs {
		remainingJobs = append(remainingJobs, jobName)
	}

	// Store this outside the loop so we don't need to allocate every time (this is probably
	// over-optimised)
	childNames := make([]string, 0, len(remainingJobs)/4)
	for {
		// Clear the global slice before running every iteration (this leaves the capacity intact)
		childNames = childNames[:0]

		initialLen := len(remainingJobs)
		// Stop if there's no remaining jobs
		if initialLen == 0 {
			break
		}

		// Collect the names of the children
		for _, parentName := range remainingJobs {
			for childName := range config.Jobs[parentName].Children {
				childNames = append(childNames, childName)
			}
		}
		remainingJobs = slices.DeleteFunc(remainingJobs, func(job string) bool {
			// If the job is a child of another job, then we must scale that job first.
			if slices.Contains(childNames, job) {
				return false
			}
			// If not, we can scale it now! So add it to our order slice and delete it from the
			// remaining jobs slice.
			order = append(order, job)
			return true
		})

		// If we didn't delete any jobs, then it's not a DAG
		if len(remainingJobs) == initialLen {
			err = fmt.Errorf("cannot determine scale order due to cycle")
			return
		}
	}

	return
}

type DeploymentTierConfig struct {
	DeploymentName string `yaml:"deploymentName"`
	MinScale       int32  `yaml:"minScale"`
	MaxScale       int32  `yaml:"maxScale"`
	SpinupTime     int32  `yaml:"spinupTime"`
	TargetTime     int32  `yaml:"targetTime"`
	// ManualSlowdownDuration sets the duration of the window for SlowDown.
	// If 0, this is set automatically based on TargetTime.
	ManualSlowdownDuration int32 `yaml:"manualSlowdownDuration"`
}

// SlowdownDuration returns the duration of the window for SlowDown.
//
// If not manually specified, this will be longer than the spinup and target time combined.
func (config *DeploymentTierConfig) SlowdownDuration(jobRunTime, qlen int32) int64 {
	duration := int64(config.ManualSlowdownDuration)
	if duration == 0 {
		// TODO: Tweak this!
		duration = int64(config.SpinupTime + config.SpinupTime/4 + config.TargetTime + config.TargetTime/4 + 1)
		// If the time taken for a single worker to complete the work is less than the window
		// duration, then we don't need to look any further back!
		singleWorkerDuration := int64(jobRunTime) * int64(qlen)
		// Except, to prevent scaling to 0, we don't go less than the jobRunTime
		if singleWorkerDuration <= 0 {
			singleWorkerDuration = int64(jobRunTime)
		}
		if singleWorkerDuration < duration {
			return singleWorkerDuration
		}
	}
	return duration
}
