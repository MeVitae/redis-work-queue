package main

import (
	"fmt"
	"go-auto-scaller-simulator/autoScallerSim"
	"go-auto-scaller-simulator/graphs"
	_ "go-auto-scaller-simulator/graphs"
	"math"
	_ "os"
)

type WorkerCounts struct {
	Base int32
	Fast int32
	Spot int32
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
	requests []autoScallerSim.WorkerCounts
	count    int
}

func NewSlowDown(count int) *SlowDown {
	return &SlowDown{
		count:    count,
		requests: make([]autoScallerSim.WorkerCounts, 0, count),
	}
}

// Push the most recent scaling request.
func (slowdown *SlowDown) Push(counts autoScallerSim.WorkerCounts) {
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
func (slowdown *SlowDown) ScaleTo(current autoScallerSim.WorkerCounts) autoScallerSim.WorkerCounts {
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

// GetCounts returns the current number of intended workers.
func GetCounts() (counts WorkerCounts, err error, workers *autoScallerSim.Workers) {
	counts.Base = workers.GetCount(workers.BaseName)
	if err != nil {
		return
	}
	counts.Fast = workers.GetCount(workers.FastName)
	if err != nil {
		return
	}
	counts.Spot = workers.GetCount(workers.SpotName)
	return
}

// GetReady returns the current number of workers which are actually ready.
func GetReady() (counts WorkerCounts, err error, workers *autoScallerSim.Workers) {
	counts.Base = workers.GetReadyCount("base")
	counts.Fast = workers.GetReadyCount("fast")
	counts.Spot = workers.GetReadyCount("spot")

	return
}

// Tick should be called on a regular time interval and will update the scaling of the workers
// accordingly.

func (autoScaller *autoScallerStruct) Tick(graph *graphs.ChartData) {
	autoScaller.workers.Mu.Lock()

	counts := autoScaller.workers.GetCounts()

	readyCounts := autoScaller.workers.GetReady()
	// Determine the current length of the work queue

	qlen := autoScaller.workers.Deployment.QueueLen()
	graph.Jobs = append(graph.Jobs, int32(qlen))
	//fmt.Printf(
	//		`Scale: Base workers: %d, Fast workers: %d, Spot workers: %d;
	//Ready: Base workers: %d, Fast workers: %d, Spot workers: %d;
	//Queue length: %d
	//`,
	//		counts.Base, counts.Fast, counts.Spot,
	//	readyCounts.Base, readyCounts.Fast, readyCounts.Spot,
	//		qlen,
	//	)
	graph.Mu.Lock()
	graph.Workers = append(graph.Workers, counts.Base+counts.Fast+counts.Spot)
	graph.ReadyWorkers = append(graph.ReadyWorkers, readyCounts.Base+readyCounts.Fast+readyCounts.Spot)
	graph.Mu.Unlock()
	newCounts := autoScaller.calculator.Calc(counts, readyCounts, int32(qlen))
	autoScaller.slowdown.Push(newCounts)
	newCounts = autoScaller.slowdown.ScaleTo(counts)
	if newCounts.Fast > autoScaller.workers.MaxFast {
		newCounts.Spot += newCounts.Fast - autoScaller.workers.MaxFast
		newCounts.Fast = autoScaller.workers.MaxFast
	}

	if newCounts.Base != counts.Base {
		//fmt.Println("Scaling base workers to", newCounts.Base)
		autoScaller.workers.SetCount(autoScaller.workers.BaseName, newCounts.Base)
	}
	if newCounts.Fast != counts.Fast {
		//fmt.Println("Scaling fast workers to", newCounts.Fast)
		autoScaller.workers.SetCount(autoScaller.workers.FastName, newCounts.Fast)
	}
	if newCounts.Spot != counts.Spot {
		//fmt.Println("Scaling spot workers to", newCounts.Spot)
		autoScaller.workers.SetCount(autoScaller.workers.SpotName, newCounts.Spot)
	}

	autoScaller.workers.ProcessWorkersChange()
	autoScaller.workers.Mu.Unlock()
}

type autoScallerStruct struct {
	workers    *autoScallerSim.Workers
	slowdown   *SlowDown
	calculator Calculator
}

func manageGraphChan(graph *map[string]*graphs.ChartData, graphData *chan graphs.GraphInfo) {
	for elem := range *graphData {
		switch elem.DataType {
		case "TotalSeconds":
			(*graph)[elem.Deployment].TotalSeconds = append((*graph)[elem.Deployment].TotalSeconds, elem.Data)
		case "Seconds":
			(*graph)[elem.Deployment].Seconds = append((*graph)[elem.Deployment].Seconds, elem.Data)
		case "Cost":
			(*graph)[elem.Deployment].Cost = append((*graph)[elem.Deployment].Cost, elem.Data) // Fixed this line
		}
	}
}

func main() {
	WorkersConfig := autoScallerSim.GetWorkersConfig("workers.yaml")
	deployments := make(autoScallerSim.SimulatedDeployments)

	config := autoScallerSim.GetConfig("config.yaml")
	tickChan := make(chan *autoScallerSim.Workers, 5)
	graphChan := make(chan graphs.GraphInfo)
	deploymentGraphs := make(map[string]*graphs.ChartData)

	autoScaller := autoScallerStruct{
		slowdown: NewSlowDown(8),
	}
	tick := 0
	//NChart := graphs.StartPlotGraph(deployment.podType)
	for index, _ := range config.MainStruct {
		fmt.Println(index)
		(deploymentGraphs)[index] = graphs.StartPlotGraph(index)
	}
	go manageGraphChan(&deploymentGraphs, &graphChan)
	go autoScallerSim.Start(&tickChan, *&config.MainStruct, WorkersConfig, &graphChan, config.ProcessStarterConfig, deployments)
	for elem := range tickChan {
		tick++
		autoScaller.workers = elem
		autoScaller.calculator = Calculator{
			Target:     int32((config.MainStruct)[elem.DepName].CalculatorY.Target),
			SpotTarget: int32((config.MainStruct)[elem.DepName].CalculatorY.SpotTarget),
			Run:        int32((config.MainStruct)[elem.DepName].CalculatorY.Run),
			Spinup:     int32((config.MainStruct)[elem.DepName].CalculatorY.Spinup),
		}
		deploymentGraphs[elem.DepName].Ticks = append(deploymentGraphs[elem.DepName].Ticks, int32(tick))
		autoScaller.Tick((deploymentGraphs)[elem.DepName])
		elem.Verify <- true
	}
}

// AutoScale autoscales the cluster! It autoscales two worker sets: section and person detection.

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
func (calc *Calculator) WillTake(counts autoScallerSim.WorkerCounts, qlen int32) int32 {
	if counts.Base+counts.Fast == 0 {
		return math.MaxInt32
	}
	return qlen * calc.Run / (counts.Base + counts.Fast)
}

// WillTake is the time it will take to get through the queue of `qlen` jobs with `counts` of
// workers including spot workers.
func (calc *Calculator) WillTakeWithSpot(counts autoScallerSim.WorkerCounts, qlen int32) int32 {
	if counts.Base+counts.Fast+counts.Spot == 0 {
		return math.MaxInt32
	}
	return qlen * calc.Run / (counts.Base + counts.Fast + counts.Spot)
}

// Calc the number of workers we'd like.
func (calc *Calculator) Calc(counts autoScallerSim.WorkerCounts, readyCounts autoScallerSim.WorkerCounts, qlen int32) autoScallerSim.WorkerCounts {
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
