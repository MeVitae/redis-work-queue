package main

import (
	"fmt"
	"sync"
)

type deploymentList struct {
	list map[string]int32
	mu   sync.RWMutex
}

type DeploymentInterfaceAutoScaler interface {
	GetScale(string) int32
	GetReadyCount(string) int32
	GetCounts(string) int32
}

type AutoScalerDb interface {
}

type AutoScalerWQ interface {
	QueueLen() int
}

type deploymentTypes []string

func (deployment *deploymentStruct) QueueLen() int {
	return len(deployment.jobs)
}

func (workers *Workers) GetScale(deploymentName string) int32 {
	workers.deploymentsList.mu.RLock()
	defer workers.deploymentsList.mu.RUnlock()
	return workers.deploymentsList.list[deploymentName]
}

func (workers *Workers) GetReadyCount(workerType string) int32 {
	realWorkers := make(map[string]int32)
	workers.deployment.mu.RLock()
	for _, worker := range workers.deployment.workers {
		if worker.timeTilStart == 0 {
			realWorkers[worker.wtype]++
		}
	}
	workers.deployment.mu.RUnlock()

	return realWorkers[workerType]
}

func (workers *Workers) GetCount(deploymentName string) int32 {
	workers.deploymentsList.mu.RLock()
	defer workers.deploymentsList.mu.RUnlock()
	return workers.deploymentsList.list[deploymentName]
}
func (workers *Workers) SetCount(deploymentName string, count int32) {
	workers.deploymentsList.mu.Lock()
	workers.deploymentsList.list[deploymentName] = count
	workers.deploymentsList.mu.Unlock()
}

func (workers *Workers) getWorkers() map[string]int32 {
	realWorkers := make(map[string]int32)

	for _, worker := range workers.deployment.workers {
		realWorkers[worker.wtype]++
	}
	return realWorkers
}

func (workers *Workers) ProcessWorkersChange() {
	deployment := workers.deployment
	realWorkers := make(map[string]int32)
	totalWorkers := workers.getWorkers()
	toDropWorkers := make(map[string]int32)
	for _, wType := range *workers.deploymentTypes {
		realWorkers[wType] = 0
		toDropWorkers[wType] = 0
	}
	Pworkers := deployment.workers

	for _, worker := range Pworkers {
		if worker.timeTilStart == 0 {
			realWorkers[worker.wtype]++
		}
		totalWorkers[worker.wtype]++
	}
	workers.deploymentsList.mu.Lock()
	for name, workers := range workers.deploymentsList.list {
		if workers > totalWorkers[name] {
			fmt.Println(name)
			DiffenceInWorkers := workers - totalWorkers[name]
			for i := int32(1); i <= DiffenceInWorkers; i++ {

				deployment.workers = append(deployment.workers, worker{
					timetilDie:   generateRandomNumber(ConfigWorker[name].timetilDieRange[0], ConfigWorker[name].timetilDieRange[1]),
					power:        1,
					workingon:    "",
					timeTilStart: generateRandomNumber(ConfigWorker[name].timeTilStart[0], ConfigWorker[name].timeTilStart[1]),
					wtype:        name,
				})
			}
		} else if workers < totalWorkers[name] {
			// dropping extra workers

			numberOfDrops := totalWorkers[name] - workers
			toDropWorkers[name] = numberOfDrops
		}
	}
	newW := []worker{}

	for _, worker := range deployment.workers {
		if worker.workingon == "" && toDropWorkers[worker.wtype] > 0 {
			toDropWorkers[worker.wtype] -= 1
		} else {
			newW = append(newW, worker)
		}
	}
	deployment.workers = newW
	workers.deploymentsList.mu.Unlock()
}
