package wqautoscale

import (
	"context"
	"fmt"
	"math"

	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	apps "k8s.io/client-go/kubernetes/typed/apps/v1"
	"k8s.io/client-go/rest"

	redis "github.com/redis/go-redis/v9"
	workqueue "github.com/mevitae/redis-work-queue/go"
)

type WorkerCounts struct {
	Base int32
	Fast int32
	Spot int32
}

// Smoother provides an interface for types which provide smoothing to scaling.
type Smoother interface {
    ScaleTo(WorkerCounts) WorkerCounts
}

// SlowDown is a Smoother which delays the downscaling of WorkerCounts without slowing down the
// upscaling.
//
// # Usage
//
// When a new scale is requested, pass this to `Push`. When applying a scale, use the `ScaleTo`
// method to retrieve the counts to actually scale to.
// Calls to the `Push` method should be performed on a regular time interval.
//
// Behaviour:
// Scaling up is not affected. Scaling down scales to the maximum of the last `count` calls to
// push. This smooths and delays scaling down.
type SlowDown struct {
	// requests stores up to the `count` most recent scale requests.
	//
	// A smaller index indicates an older request.
	requests []WorkerCounts
	count    int
}

func NewSlowDown(count int) *SlowDown {
	return &SlowDown{
		count:    count,
		requests: make([]WorkerCounts, 0, count),
	}
}

// Push the most recent scaling request.
func (slowdown *SlowDown) Push(counts WorkerCounts) {
	// Store only the last `count` requests. Keeping the newest request at the end.
	if len(slowdown.requests) < slowdown.count {
		slowdown.requests = append(slowdown.requests, counts)
	} else {
		// Make sure the length is equal to `count`.
		slowdown.requests = slowdown.requests[len(slowdown.requests)-slowdown.count:]
		// Shift everything left by 1
		copy(slowdown.requests, slowdown.requests[1:])
		// Insert the item at the end
		slowdown.requests[len(slowdown.requests)-1] = counts
	}
}

// ScaleTo decides what the worker counts should actually be scaled to.
//
// See the docs on `SlowDown` for how this behaves.
func (slowdown *SlowDown) ScaleTo(current WorkerCounts) WorkerCounts {
	ret := slowdown.requests[len(slowdown.requests)-1]

	if ret.Base < current.Base {
		// If scaling down, we scale down to the max of the recent requests
		for idx := 1; idx < len(slowdown.requests); idx++ {
			this := slowdown.requests[idx].Base
			if this > ret.Base {
				ret.Base = this
			}
		}
	}

	if ret.Fast < current.Fast {
		// If scaling down, we scale down to the max of the recent requests
		for idx := 1; idx < len(slowdown.requests); idx++ {
			this := slowdown.requests[idx].Fast
			if this > ret.Fast {
				ret.Fast = this
			}
		}
	}

	if ret.Spot < current.Spot {
		// If scaling down, we scale down to the max of the recent requests
		for idx := 1; idx < len(slowdown.requests); idx++ {
			this := slowdown.requests[idx].Spot
			if this > ret.Spot {
				ret.Spot = this
			}
		}
	}

	return ret
}

// Workers are a set of 3 k8s deployments: base, fast and spot. These are responsible for handling the
// jobs from a job queue in a Redis database.
type Workers struct {
	// deployments is the interface to get and set information about the k8s deployment.
	deployments apps.DeploymentInterface
	// baseName of the base k8s deployment.
	baseName string
	// fastName of the fast k8s deployment.
	fastName string
	// spotName of the spot k8s deployment.
	spotName string

	// db is the Redis database containing the work queue.
	db *redis.Client
	// queue is the work queue to scale for
	queue *workqueue.WorkQueue

	// calculator is the function to calculate scaling values.
	calculator Calculator
	// slowdown is the SlowDown instance for delaying and smoothing downscale.
	slowdown *SlowDown
	// maxFast is the maximum number of fast workers until they are converted to spot workers.
	//
	// maxFast is stored here since it's taken into account *after* the slowdown is applied, not in
	// the Calculator.
	maxFast int32
}

func NewWorkers(
	deployments apps.DeploymentInterface,
	namePrefix string,

	db *redis.Client,
	queue *workqueue.WorkQueue,

	calculator Calculator,
	maxFast int32,
) Workers {
	return Workers{
		deployments: deployments,
		baseName:    namePrefix + "-workers-base",
		fastName:    namePrefix + "-workers-fast",
		spotName:    namePrefix + "-workers-spot",

		db:    db,
		queue: queue,

		calculator: calculator,
		slowdown:   NewSlowDown(8),
		maxFast:    maxFast,
	}
}

// getCount returns the current number of intended workers.
func (workers *Workers) getCount(ctx context.Context, deploymentName string) (int32, error) {
	scale, err := workers.deployments.GetScale(ctx, deploymentName, meta.GetOptions{})
	if err != nil {
		return 0, err
	}
	return scale.Spec.Replicas, nil
}

// getReadyCount returns the current number of workers which are actually ready.
func (workers *Workers) getReadyCount(ctx context.Context, deploymentName string) (int32, error) {
	deployment, err := workers.deployments.Get(ctx, deploymentName, meta.GetOptions{})
	if err != nil {
		return 0, err
	}
	return deployment.Status.ReadyReplicas, nil
}

// setCount sets the intended scaling of the workers.
func (workers *Workers) setCount(ctx context.Context, deploymentName string, count int32) error {
	scale, err := workers.deployments.GetScale(ctx, deploymentName, meta.GetOptions{})
	if err != nil {
		return err
	}
	scale.Spec.Replicas = count
	_, err = workers.deployments.UpdateScale(ctx, deploymentName, scale, meta.UpdateOptions{})
	return err
}

// GetCounts returns the current number of intended workers.
func (workers *Workers) GetCounts(ctx context.Context) (counts WorkerCounts, err error) {
	counts.Base, err = workers.getCount(ctx, workers.baseName)
	if err != nil {
		return
	}
	counts.Fast, err = workers.getCount(ctx, workers.fastName)
	if err != nil {
		return
	}
	counts.Spot, err = workers.getCount(ctx, workers.spotName)
	return
}

// GetReadyCounts returns the current number of workers which are actually ready.
func (workers *Workers) GetReadyCounts(ctx context.Context) (counts WorkerCounts, err error) {
	counts.Base, err = workers.getReadyCount(ctx, workers.baseName)
	if err != nil {
		return
	}
	counts.Fast, err = workers.getReadyCount(ctx, workers.fastName)
	if err != nil {
		return
	}
	counts.Spot, err = workers.getReadyCount(ctx, workers.spotName)
	return
}

// Tick should be called on a regular time interval and will update the scaling of the workers
// accordingly.
func (workers *Workers) Tick(ctx context.Context) error {
	counts, err := workers.GetCounts(ctx)
	if err != nil {
		return err
	}
	readyCounts, err := workers.GetReadyCounts(ctx)
	if err != nil {
		return err
	}
	// Determine the current length of the work queue
    qlen, err := workers.queue.QueueLen(ctx, workers.db)
	if err != nil {
		return err
	}
	// Include jobs being worked on in the length of the queue - this can prevent sharp downscaling.
	plen, err := workers.queue.Processing(ctx, workers.db)
	if err != nil {
		return err
	}
	qlen += plen

	fmt.Printf(
		`Scale: Base workers: %d, Fast workers: %d, Spot workers: %d;
Ready: Base workers: %d, Fast workers: %d, Spot workers: %d;
Queue length: %d (processing: %d)
`,
		counts.Base, counts.Fast, counts.Spot,
		readyCounts.Base, readyCounts.Fast, readyCounts.Spot,
		qlen, plen,
	)

	newCounts := workers.calculator.Calc(counts, readyCounts, int32(qlen))
	workers.slowdown.Push(newCounts)
	newCounts = workers.slowdown.ScaleTo(counts)
	if newCounts.Fast > workers.maxFast {
		newCounts.Spot += newCounts.Fast - workers.maxFast
		newCounts.Fast = workers.maxFast
	}

	if newCounts.Base != counts.Base {
		fmt.Println("Scaling base workers to", newCounts.Base)
		workers.setCount(ctx, workers.baseName, newCounts.Base)
	}
	if newCounts.Fast != counts.Fast {
		fmt.Println("Scaling fast workers to", newCounts.Fast)
		workers.setCount(ctx, workers.fastName, newCounts.Fast)
	}
	if newCounts.Spot != counts.Spot {
		fmt.Println("Scaling spot workers to", newCounts.Spot)
		workers.setCount(ctx, workers.spotName, newCounts.Spot)
	}

	return nil
}

// AutoScale autoscales the cluster! It autoscales two worker sets: section and person detection.
type AutoScale struct {
	clientset *kubernetes.Clientset
	// config for connecting to k8s
	config *rest.Config
	// namespace of the deployments in k8s
	namespace string

	// section detection workers
	section Workers
	// person detection workers
	person Workers
	// reading order workers
	reading Workers
	// section classification workers
	sectionClass Workers
	// feature extraction workers
	featureExtraction Workers
}

// Tick should be called repeatedly to scale the cluster.
func (autoscale *AutoScale) Tick(ctx context.Context) error {
	fmt.Println("Scaling person detection")
	err := autoscale.person.Tick(ctx)
	if err != nil {
		return err
	}
	fmt.Println("Scaling section detection")
	err = autoscale.section.Tick(ctx)
	if err != nil {
		return err
	}
	fmt.Println("Scaling reading order")
	err = autoscale.reading.Tick(ctx)
	if err != nil {
		return err
	}
	fmt.Println("Scaling section classification")
	err = autoscale.sectionClass.Tick(ctx)
	if err != nil {
		return err
	}
	fmt.Println("Scaling feature extraction")
	err = autoscale.featureExtraction.Tick(ctx)
	if err != nil {
		return err
	}
	return nil
}

// Calculator calculates what to scale the workers to!
//
// Units within the structure are only relative to each-other (i.e. that could be seconds, minutes,
// or whatever you want as long as they're consistent).
type Calculator struct {
	// Target time to run through all the jobs in the queue (excluding spot instances).
	Target int32
	// Target is the target time to run through all the jobs in the queue including the use of spot
	// instances.
	SpotTarget int32

	// Run is the time is takes for one job to run on one worker.
	Run int32
	// Spinup is the approximate time between requesting a scale up to having all the workers
	// available.
	Spinup int32
}

// WillTake is the time it will take to get through the queue of `qlen` jobs with `counts` of
// workers, excluding spot workers.
func (calc *Calculator) WillTake(counts WorkerCounts, qlen int32) int32 {
	if counts.Base+counts.Fast == 0 {
		return math.MaxInt32
	}
	return qlen * calc.Run / (counts.Base + counts.Fast)
}

// WillTake is the time it will take to get through the queue of `qlen` jobs with `counts` of
// workers including spot workers.
func (calc *Calculator) WillTakeWithSpot(counts WorkerCounts, qlen int32) int32 {
	if counts.Base+counts.Fast+counts.Spot == 0 {
		return math.MaxInt32
	}
	return qlen * calc.Run / (counts.Base + counts.Fast + counts.Spot)
}

// Calc the number of workers we'd like.
func (calc *Calculator) Calc(counts WorkerCounts, readyCounts WorkerCounts, qlen int32) WorkerCounts {
	// If the queue is empty, we don't need any workers (other than the base workers)!
	if qlen == 0 {
		counts.Fast = 0
		counts.Spot = 0
		return counts
	}

	// The estimated length of the queue after the spinup time
	shorterQlen := qlen - (readyCounts.Fast+readyCounts.Base)*calc.Spinup/calc.Run
	shorterQlenSpot := qlen - (readyCounts.Fast+readyCounts.Base+readyCounts.Spot)*calc.Spinup/calc.Run

	willTake := calc.WillTake(readyCounts, qlen)
	willTakeWhenReady := calc.WillTake(counts, qlen)
	if willTakeWhenReady > calc.Spinup && willTakeWhenReady > calc.Target {
		// Want willTake = Target
		//   so qlen * Run / (Base + Fast) = Target
		//   => Bast + Fast = qlen * Run / Target
		newFast := shorterQlen*calc.Run/calc.Target - counts.Base
		if newFast > counts.Fast {
			counts.Fast = newFast
		}
	} else if willTake < calc.Target {
		counts.Fast = qlen*calc.Run/calc.Target - counts.Base
	}

	if counts.Fast < 0 {
		counts.Fast = 0
	}
	// Make sure we have at least 1 worker
	if (counts.Base == 0 || readyCounts.Base == 0) && counts.Fast == 0 {
		counts.Fast = 1
	}

	willTakeSpot := calc.WillTakeWithSpot(readyCounts, qlen)
	willTakeSpotWhenReady := calc.WillTakeWithSpot(counts, qlen)
	if willTakeWhenReady > calc.Spinup && willTakeSpotWhenReady > calc.SpotTarget {
		// Similar to above
		newSpot := shorterQlenSpot*calc.Run/calc.SpotTarget - counts.Base - counts.Fast
		if newSpot > counts.Spot {
			counts.Spot = newSpot
		}
	} else if willTakeSpot < calc.SpotTarget {
		counts.Spot = qlen*calc.Run/calc.SpotTarget - counts.Base - counts.Fast
	}
	if counts.Spot < 0 {
		counts.Spot = 0
	}

	return counts
}
