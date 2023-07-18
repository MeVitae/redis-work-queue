package main

import (
	"math"
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
	requests []map[string]int32
	count    int
}

func NewSlowDown(count int) *SlowDown {
	return &SlowDown{
		count:    count,
		requests: make([]map[string]int32, 0, count),
	}
}

// Push the most recent scaling request.
func (slowdown *SlowDown) Push(counts map[string]int32) {
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
func (slowdown *SlowDown) ScaleTo(current map[string]int32) map[string]int32 {
	ret := slowdown.requests[len(slowdown.requests)-1]

	if ret["Base"] < current["Base"] {
		// If scaling down, we scale down to the max of the recent requests
		for idx := 1; idx < len(slowdown.requests); idx++ {
			this := slowdown.requests[idx]["Base"]
			if this > ret["Base"] {
				ret["Base"] = this
			}
		}
	}

	if ret["Fast"] < current["Fast"] {
		// If scaling down, we scale down to the max of the recent requests
		for idx := 1; idx < len(slowdown.requests); idx++ {
			this := slowdown.requests[idx]["Fast"]
			if this > ret["Fast"] {
				ret["Fast"] = this
			}
		}
	}

	if ret["Spot"] < current["Spot"] {
		// If scaling down, we scale down to the max of the recent requests
		for idx := 1; idx < len(slowdown.requests); idx++ {
			this := slowdown.requests[idx]["Spot"]
			if this > ret["Spot"] {
				ret["Spot"] = this
			}
		}
	}

	return ret
}

// Workers are a set of 3 k8s deployments: base, fast and spot. These are responsible for handling the
// jobs from a job queue in a Redis database.
type Workers struct {
	// deployments is the interface to get and set information about the k8s deployment.
	// baseName of the base k8s deployment.
	baseName string
	// fastName of the fast k8s deployment.
	fastName string
	// spotName of the spot k8s deployment.
	spotName string

	currentSlowDown     int
	consecutiveSlowDown int
	// db is the Redis database containing the work queue.

	// queue is the work queue to scale for
	queue *map[string]int32

	// calculator is the function to calculate scaling values.
	calculator Calculator
	// slowdown is the SlowDown instance for delaying and smoothing downscale.
	slowdown      *SlowDown
	currentUP     int
	consecutiveUP int

	// maxFast is the maximum number of fast workers until they are converted to spot workers.
	//
	// maxFast is stored here since it's taken into account *after* the slowdown is applied, not in
	// the Calculator.
	maxFast int32
}

// Tick should be called on a regular time interval and will update the scaling of the workers
// accordingly.

func (workers *Workers) Tick(WorkersCounts map[string]int32, joblen int32, readyCounts map[string]int32) map[string]int32 {
	counts := WorkersCounts
	qlen := joblen

	//fmt.Println(qlen)
	//fmt.Printf(
	//		`Scale: Base workers: %d, Fast workers: %d, Spot workers: %d;
	//Ready: Base workers: %d, Fast workers: %d, Spot workers: %d;
	//Queue length: %d
	//`,
	//	counts["Base"], counts["Fast"], counts["Spot"],
	//	readyCounts["Base"], readyCounts["Fast"], readyCounts["Spot"],
	//	qlen,
	//)

	newCounts := workers.calculator.Calc(counts, readyCounts, int32(qlen))
	workers.slowdown.Push(newCounts)
	newCounts = workers.slowdown.ScaleTo(newCounts)
	if newCounts["Fast"] > workers.maxFast {
		newCounts["Spot"] += newCounts["Fast"] - workers.maxFast
		newCounts["Fast"] = workers.maxFast
	}

	return newCounts
}

// AutoScale autoscales the cluster! It autoscales two worker sets: section and person detection.

// InClusterAutoScale returns an autoscaler which scales the cluster the pod running in!

// Tick should be called repeatedly to scale the cluster.

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
func (calc *Calculator) WillTake(counts map[string]int32, qlen int32) int32 {
	if counts["Base"]+counts["Fast"] == 0 {
		return math.MaxInt32
	}
	return qlen * calc.Run / (counts["Base"] + counts["Fast"])
}

// WillTake is the time it will take to get through the queue of `qlen` jobs with `counts` of
// workers including spot workers.
func (calc *Calculator) WillTakeWithSpot(counts map[string]int32, qlen int32) int32 {
	if counts["Base"]+counts["Fast"]+counts["Spot"] == 0 {
		return math.MaxInt32
	}
	return qlen * calc.Run / (counts["Base"] + counts["Fast"] + counts["Spot"])
}

// Calc the number of workers we'd like.
func (calc *Calculator) Calc(counts map[string]int32, readyCounts map[string]int32, qlen int32) map[string]int32 {
	// If the queue is empty, we don't need any workers (other than the base workers)!
	if qlen == 0 {
		counts["Fast"] = 0
		counts["Spot"] = 0
		return counts
	}

	//fmt.Println("counts:", counts, "Rcounts", readyCounts, "Qlen", qlen)
	// The estimated length of the queue after the spinup time
	shorterQlen := qlen - (readyCounts["Fast"]+readyCounts["Base"])*calc.Spinup/calc.Run
	shorterQlenSpot := qlen - (readyCounts["Fast"]+readyCounts["Base"]+readyCounts["Spot"])*calc.Spinup/calc.Run

	willTake := calc.WillTake(readyCounts, qlen)
	willTakeWhenReady := calc.WillTake(counts, qlen)
	if willTakeWhenReady > calc.Spinup && willTakeWhenReady > calc.Target {
		// Want willTake = Target
		//		//   so qlen * Run / (Base + Fast) = Target
		//		//   => Bast + Fast = qlen * Run / Target
		newFast := shorterQlen*calc.Run/calc.Target - counts["Base"]
		if newFast > counts["Fast"] {
			counts["Fast"] = newFast
		}
	} else if willTake < calc.Target {
		counts["Fast"] = qlen*calc.Run/calc.Target - counts["Base"]
	}

	if counts["Fast"] < 0 {
		counts["Fast"] = 0
	}
	// Make sure we have at least 1 worker
	if (counts["Base"] == 0 || readyCounts["Base"] == 0) && counts["Fast"] == 0 {
		counts["Fast"] = 1
	}

	willTakeSpot := calc.WillTakeWithSpot(readyCounts, qlen)
	willTakeSpotWhenReady := calc.WillTakeWithSpot(counts, qlen)
	if willTakeWhenReady > calc.Spinup && willTakeSpotWhenReady > calc.SpotTarget {
		// Similar to above
		newSpot := shorterQlenSpot*calc.Run/calc.SpotTarget - counts["Base"] - counts["Fast"]
		if newSpot > counts["Spot"] {
			counts["Spot"] = newSpot
		}
	} else if willTakeSpot < calc.SpotTarget {
		counts["Spot"] = qlen*calc.Run/calc.SpotTarget - counts["Base"] - counts["Fast"]
	}
	if counts["Spot"] < 0 {
		counts["Spot"] = 0
	}

	return counts
}
