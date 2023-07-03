package main

import (
	"fmt"
)

type workerConfig struct {
	assignedJob      string
	powerRange       [2]float32
	costRange        [2]float32
	timeTilDropRange [2]int
	startupTimeRange [2]int
}

type worker struct {
	myJob       currentJob
	power       float32
	cost        float32
	timeTilDrop int
	startupTime int
}

//Power per 10 miliseconds
//

var workerTypes = map[string]workerConfig{
	"Base": workerConfig{
		powerRange:       [2]float32{0.8, 1.2},
		costRange:        [2]float32{0.8, 1.2},
		timeTilDropRange: [2]int{-1, -1},
		startupTimeRange: [2]int{5, 100},
	},
	"Spot": workerConfig{
		powerRange:       [2]float32{0.8, 1.2},
		costRange:        [2]float32{0.4, 1.0},
		timeTilDropRange: [2]int{2000, 20000},
		startupTimeRange: [2]int{1500, 2500},
	},
	"Fast": workerConfig{
		powerRange:       [2]float32{2.0, 7.0},
		costRange:        [2]float32{2.0, 4.0},
		timeTilDropRange: [2]int{-1, -1},
		startupTimeRange: [2]int{1000, 1800},
	},
}

func createWorker(Type string) worker {
	workerType := workerTypes[Type]
	fmt.Println(workerType.powerRange[0])
	var worker = worker{
		myJob:       currentJob{},
		power:       generateRandomFloat(workerType.powerRange[0], workerType.powerRange[1]),
		cost:        generateRandomFloat(workerType.costRange[0], workerType.costRange[1]),
		timeTilDrop: generateRandomNumber(workerType.timeTilDropRange[0], workerType.timeTilDropRange[1]),
		startupTime: generateRandomNumber(workerType.startupTimeRange[0], workerType.startupTimeRange[1]),
	}
	return worker
}

func addWorker(worker worker) {
	jobQ.workers = append(jobQ.workers, worker)
	fmt.Printf("Added Worker: %s\n", worker.myJob)
}

func dropWorker(workerType string) {

}
