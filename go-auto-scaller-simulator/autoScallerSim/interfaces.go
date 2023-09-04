package autoScallerSim

import (
	"sync"
)

type deploymentList struct {
	list map[string]int32
	mu   sync.RWMutex
}

type DeploymentInterfaceAutoScaler interface {
	GetScale(string) int32
	GetReadyCounts() WorkerCounts
	GetCounts() WorkerCounts
}

type WorkerCounts struct {
	Base int32
	Fast int32
	Spot int32
}

func (workers *Workers) GetReadyCounts() (counts WorkerCounts) {
	counts.Base = workers.GetReadyCount("base")
	counts.Fast = workers.GetReadyCount("fast")
	counts.Spot = workers.GetReadyCount("spot")

	return
}

func (workers *Workers) GetCounts() (counts WorkerCounts) {
	counts.Base = workers.GetCount(workers.BaseName)
	counts.Fast = workers.GetCount(workers.FastName)
	counts.Spot = workers.GetCount(workers.SpotName)
	return
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
	workers.Deployment.mu.RLock()
	for _, worker := range workers.Deployment.workers {
		if worker.timeTilStart == 0 {
			realWorkers[worker.wtype]++
		}
	}
	workers.Deployment.mu.RUnlock()

	return realWorkers[workerType]
}

func (workers *Workers) GetCount(deploymentName string) int32 {
	workers.deploymentsList.mu.RLock()
	defer workers.deploymentsList.mu.RUnlock()
	return workers.deploymentsList.list[deploymentName]
}
func (workers *Workers) SetCount(deploymentName string, count int32) {
	if count > 90 {
		count = 90
	}
	workers.deploymentsList.mu.Lock()
	workers.deploymentsList.list[deploymentName] = count
	workers.deploymentsList.mu.Unlock()
}

func (workers *Workers) getWorkers() map[string]int32 {
	realWorkers := make(map[string]int32)

	for _, worker := range workers.Deployment.workers {
		realWorkers[worker.wtype]++
	}
	return realWorkers
}

func (workers *Workers) ProcessWorkersChange() {
	deployment := workers.Deployment
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
	for name, Workers := range workers.deploymentsList.list {
		if Workers > totalWorkers[name] {
			DiffenceInWorkers := Workers - totalWorkers[name]
			for i := int32(1); i <= DiffenceInWorkers; i++ {
				//fmt.Println(workers.WorkersConfig)
				deployment.workers = append(deployment.workers, worker{
					timetilDie:   generateRandomNumber(workers.WorkersConfig[name].TimetilDieRange[0], workers.WorkersConfig[name].TimetilDieRange[1]),
					power:        1,
					workingon:    "",
					cost:         workers.WorkersConfig[name].Price,
					timeTilStart: generateRandomNumber(workers.WorkersConfig[name].TimeTilStart[0], workers.WorkersConfig[name].TimeTilStart[1]),
					wtype:        name,
				})
			}
		} else if Workers < totalWorkers[name] {
			numberOfDrops := totalWorkers[name] - Workers
			toDropWorkers[name] = numberOfDrops
		}
	}
	newW := []worker{}

	for _, worker := range deployment.workers {
		if worker.workingon == "" && toDropWorkers[worker.wtype] > 0 && worker.timeTilStart == 0 {
			toDropWorkers[worker.wtype] -= 1
		} else {
			newW = append(newW, worker)
		}
	}
	deployment.workers = newW
	workers.deploymentsList.mu.Unlock()
}

type Workers struct {
	Mu sync.RWMutex
	// deployments is the interface to get and set information about the k8s deployment.
	Deployments DeploymentInterfaceAutoScaler
	// baseName of the base k8s deployment.
	BaseName string
	// fastName of the fast k8s deployment.
	FastName string
	// spotName of the spot k8s deployment.
	SpotName string

	Verify        chan bool
	WorkersConfig map[string]WorkerConfig
	DepName       string
	Deployment    *deploymentStruct

	deploymentTypes *deploymentTypes
	// db is the Redis database containing the work queue.
	db AutoScalerDb
	// queue is the work queue to scale for
	queue *map[string]int32

	QueueManager    AutoScalerWQ
	deploymentsList deploymentList

	MaxFast int32
}

func NewWorkers(deployment *deploymentStruct, finishjob chan job, Config Worker, WorkersConfig map[string]WorkerConfig) Workers {
	return Workers{
		deploymentTypes: &deploymentTypes{"base,spot,fast"},
		BaseName:        "base",
		FastName:        "fast",
		SpotName:        "spot",
		queue:           &map[string]int32{},
		deploymentsList: deploymentList{
			list: make(map[string]int32),
		},
		DepName:       deployment.podType,
		Verify:        make(chan bool),
		WorkersConfig: WorkersConfig,
		Deployment:    deployment,
		MaxFast:       96,
	}
}
