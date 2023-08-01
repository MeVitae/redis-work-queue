package main

import (
	"fmt"

	//	_ "net/http/pprof"
	"strconv"
	"sync"

	"github.com/google/uuid"
)

type document struct {
	pages  int
	images int
	id     string
}

type progress struct {
	haveToBeDone int
	done         int
}

type SafeCounter struct {
	mu       sync.RWMutex
	progress map[string]progress
}

var maxNumberOfJobs = 10000

var docsNeedToBeDone = SafeCounter{progress: make(map[string]progress)}

func createNewDocToProcess(chanel map[string]deploymentStruct) {
	docsNeedToBeDone.mu.RLock()
	if len(docsNeedToBeDone.progress) > maxNumberOfJobs {
		docsNeedToBeDone.mu.RUnlock()
		return
	}
	docsNeedToBeDone.mu.RUnlock()
	doc := document{
		pages:  generateRandomNumber(1, 3),
		images: 0,
		id:     uuid.New().String(),
	}
	if generateRandomBool(1, 4) {
		doc.images = generateRandomNumber(1, 24)
	}

	// Lock so only one goroutine at a time can access the map c.v.
	docsNeedToBeDone.mu.Lock()
	docsNeedToBeDone.progress[doc.id] = progress{}
	docsNeedToBeDone.mu.Unlock()
	for i := 0; i < doc.images; i++ {
		chanel["person"].jobChan <- incomingJob{name: "PSD", time: 0, startAt: tick, myData: doc}
	}
	for i := 0; i < doc.pages; i++ {
		chanel["signature"].jobChan <- incomingJob{name: "SD", time: 0, startAt: tick, myData: doc}
		chanel["section"].jobChan <- incomingJob{name: "SD", time: 0, startAt: tick, myData: doc}

	}

}

//  1000 ticks == 1 second

type worker struct {
	timetilDie   int
	timeTilStart int
	power        int
	workingon    string
	wtype        string
}

type job struct {
	startedAt   int
	powerNeeded int
	taken       bool
	myTicks     int
	jobType     string
	MyData      document
	startAt     int32
}

// TODO: rename to deployment

type incomingJob struct {
	time    int
	name    string
	startAt int
	myData  document
}

type PowerRange struct {
	Min int
	Max int
}

type JobConfig struct {
	PowerNeededRange [2]int
}
type workerConfig struct {
	timetilDieRange [2]int
	timeTilStart    [2]int
	price           float64
}

// 10 ticks = 1 sec

var Config = map[string]JobConfig{
	"PSD": {
		PowerNeededRange: [2]int{70, 90},
	},
	"SD": {
		PowerNeededRange: [2]int{40, 60},
	},
	"RO": {
		PowerNeededRange: [2]int{2, 5},
	},
	"NER": {
		PowerNeededRange: [2]int{300, 400},
	},
	"PREProcessor": {
		PowerNeededRange: [2]int{100, 140},
	},
	"SGD": {
		PowerNeededRange: [2]int{60, 100},
	},
	"SC": {
		PowerNeededRange: [2]int{8, 40},
	},
	"FE": {
		PowerNeededRange: [2]int{1, 4},
	},
}

var ConfigWorker = map[string]workerConfig{
	"Base": {
		timetilDieRange: [2]int{-1, -1},
		timeTilStart:    [2]int{2200, 2400},
		price:           1,
	},
	"Spot": {
		timetilDieRange: [2]int{130000, 290000},
		timeTilStart:    [2]int{900, 1400},
		price:           0.15,
	},
	"Fast": {
		timetilDieRange: [2]int{-1, -1},
		timeTilStart:    [2]int{900, 1400},
		price:           1.2,
	},
}

type workersCount struct {
	workers map[string]int32
}

var tick = 1

func getWorkers(pod deploymentStruct) (realWorkers map[string]int32, totalWorkers map[string]int32) {
	realWorkers = make(map[string]int32)
	totalWorkers = make(map[string]int32)
	realWorkers["Base"] = 0
	realWorkers["Spot"] = 0
	realWorkers["Fast"] = 0

	totalWorkers["Base"] = 0
	totalWorkers["Spot"] = 0
	totalWorkers["Fast"] = 0
	for _, worker := range pod.workers {
		if worker.timeTilStart == 0 {
			realWorkers[worker.wtype]++
		}
		totalWorkers[worker.wtype]++
	}
	return
}

func receiveJobs(Pod *deploymentStruct, incomingJobChan chan incomingJob, name string) {
	for jobID := range incomingJobChan {
		uuid := uuid.New()
		Pod.mu.Lock()
		Pod.jobs[uuid.String()] = job{
			startedAt:   jobID.startAt,
			powerNeeded: generateRandomNumber(Config[name].PowerNeededRange[0], Config[name].PowerNeededRange[1]),
			taken:       false,
			myTicks:     jobID.time,
			jobType:     name,
			MyData:      jobID.myData,
			startAt:     int32(tick),
		}
		Pod.mu.Unlock()

		docsNeedToBeDone.mu.Lock()
		doc := docsNeedToBeDone.progress[jobID.myData.id]
		doc.haveToBeDone += 1
		docsNeedToBeDone.progress[jobID.myData.id] = doc
		docsNeedToBeDone.mu.Unlock()
	}
}

var hardcCodedLimits = make(map[string]int32)

func Deployment(deployment *deploymentStruct, finishjob chan job, calcConfig CalculatorY) {
	myStats := statsStruct{}
	workers := Workers{
		baseName: "base",
		fastName: "fast",
		spotName: "spot",
		queue:    &map[string]int32{},
		calculator: Calculator{
			Target:     int32(calcConfig.Target),
			SpotTarget: int32(calcConfig.SpotTarget),
			Run:        int32(calcConfig.Run),
			Spinup:     int32(calcConfig.Spinup),
		},
		CData:    startPlotGraph(deployment.podType),
		slowdown: NewSlowDown(8),
		maxFast:  96,
	}

	toDropWorkers := make(map[string]int32)

	go receiveJobs(deployment, deployment.jobChan, deployment.podType)
	//Pod.workers = append(Pod.workers, worker{timetilDie: -1, power: 1, workingon: "", timeTilStart: 140000})
	fmt.Println(deployment.podType, "is ready")
	for tick := range deployment.tickChan {
		deployment.mu.Lock()

		if tick%500 == 0 {

			//		fmt.Println(Pod)
			realWorkers := make(map[string]int32)
			totalWorkers := make(map[string]int32)
			totalWorkers2 := make(map[string]int32)
			realWorkers["Base"] = 0
			realWorkers["Spot"] = 0
			realWorkers["Fast"] = 0

			totalWorkers["Base"] = 0
			totalWorkers["Spot"] = 0
			totalWorkers["Fast"] = 0

			totalWorkers2["Base"] = 0
			totalWorkers2["Spot"] = 0
			totalWorkers2["Fast"] = 0

			toDropWorkers["Base"] = 0
			toDropWorkers["Spot"] = 0
			toDropWorkers["Fast"] = 0

			Pworkers := deployment.workers

			for _, worker := range Pworkers {
				if worker.timeTilStart == 0 {
					realWorkers[worker.wtype]++
				}
				totalWorkers[worker.wtype]++
				totalWorkers2[worker.wtype]++
			}
			newc := workers.Tick(totalWorkers, int32(len(deployment.jobs)), realWorkers)
			//	fmt.Println(newc, deployment.podType)
			for name, workers := range newc {

				//fmt.Println("Pod Workers", Pod.workers)
				//fmt.Println("New workers:", workers, "Acctual workers:", totalWorkers2[name])
				if workers > totalWorkers2[name] {
					DiffenceInWorkers := workers - totalWorkers2[name]
					//	fmt.Println("Creating ", DiffenceInWorkers, " workers of type", name)

					for i := int32(1); i <= DiffenceInWorkers; i++ {

						deployment.workers = append(deployment.workers, worker{timetilDie: generateRandomNumber(ConfigWorker[name].timetilDieRange[0], ConfigWorker[name].timetilDieRange[1]), power: 1, workingon: "", timeTilStart: generateRandomNumber(ConfigWorker[name].timeTilStart[0], ConfigWorker[name].timeTilStart[1]), wtype: name})
						//fmt.Println(Pod.workers)
					}
				} else if workers < totalWorkers2[name] {
					// dropping extra workers

					numberOfDrops := totalWorkers2[name] - workers
					toDropWorkers[name] = numberOfDrops
					if numberOfDrops > 0 {
						//	fmt.Println("Have to drop", totalWorkers2[name], name, "workers", deployment.podType)
					}
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
			stats.mu.RLock()

			meantime := -1

			if myStats.jobsDone != 0 && myStats.jobsDone != 0 {
				meantime = myStats.seconds/myStats.jobsDone + 1
				workers.CData.ticks = append(workers.CData.ticks, int32(tick/10))
				workers.addSeconds(int32(meantime))
				myStats.jobsDone = 0
				myStats.seconds = 0
			}

			meantimeTotal := -1
			if stats.jobsDone != 0 && stats.jobsDone != 0 {
				meantimeTotal = stats.seconds/stats.jobsDone + 1
				workers.addSecondsTotal(int32(meantimeTotal))
			}
			stats.mu.RUnlock()
			if tick%1500 == 0 {
				fmt.Println("View: ", ":8081/", deployment.podType, "| number of jobs:", strconv.Itoa(len(deployment.jobs)), "| number of workers:", len(deployment.workers), "| deployment:", deployment.podType, "| job done mean time overall:", meantime)
			}
		}

		for id, job := range deployment.jobs {
			job.myTicks += 1
			deployment.jobs[id] = job
		}
		for Wid, worker := range deployment.workers {

			if worker.timeTilStart > 0 {
				worker.timeTilStart -= 1
				//fmt.Println(worker.timeTilStart)
				deployment.workers[Wid] = worker
			} else if worker.workingon == "" {

				for id, job := range deployment.jobs {
					if !job.taken {
						job.taken = true
						deployment.jobs[id] = job
						deployment.workers[Wid].workingon = id
						break
					}
				}

			} else {
				myjob := deployment.jobs[worker.workingon]
				if myjob.powerNeeded%100 == 0 {
					//fmt.Println(myjob.powerNeeded)
				}
				if myjob.powerNeeded > 0 {
					myjob.powerNeeded -= worker.power
					deployment.jobs[worker.workingon] = myjob
				} else {
					//fmt.Println("finish")

					docsNeedToBeDone.mu.Lock()
					doc := docsNeedToBeDone.progress[myjob.MyData.id]
					doc.done += 1
					myStats.jobsDone++
					myStats.seconds += tick/10 - deployment.jobs[worker.workingon].startedAt/10
					docsNeedToBeDone.progress[myjob.MyData.id] = doc
					docsNeedToBeDone.mu.Unlock()
					//")

					//	fmt.Println(nameP)
					finishjob <- deployment.jobs[worker.workingon]
					delete(deployment.jobs, worker.workingon)
					deployment.workers[Wid].workingon = ""
				}
			}
		}
		deployment.mu.Unlock()
	}
}

type deploymentStruct struct {
	mu       sync.RWMutex
	workers  []worker
	jobs     map[string]job
	podType  string
	jobChan  chan incomingJob
	tickChan chan int
}

func makeDepoloyment(name string, incomingJobCh *commingJobs) (deployment *deploymentStruct) {
	deployment = &deploymentStruct{
		workers:  []worker{},
		jobs:     make(map[string]job),
		podType:  name,
		jobChan:  make(chan incomingJob, 64000),
		tickChan: make(chan int, 1), // change buffer size to change the allowed number of tick desyncs
	}

	incomingJobCh.mu.Lock()
	incomingJobCh.JChan[name] = *deployment
	incomingJobCh.mu.Unlock()
	return
}

type commingJobs struct {
	mu    sync.Mutex
	JChan map[string]deploymentStruct
}

func main() {
	config := readYaml()

	hardcCodedLimits["Base"] = 1599
	hardcCodedLimits["Spot"] = 1599
	hardcCodedLimits["Fast"] = 1599

	// Person and signature detection (TODO: split up)
	incommingChans := commingJobs{
		JChan: make(map[string]deploymentStruct),
	}

	jobFinish := make(chan job)
	for index, _ := range config {
		fmt.Println(index)
		go Deployment(makeDepoloyment(index, &incommingChans), jobFinish, config[index].CalculatorY)
	}

	tikTimingJobs := getTickJobTimings()
	//go createNewDocToProcess(incomingJobPSD, incomingJobSD)

	go jobFinishF(jobFinish, incommingChans, config)
	for {

		if tikTimingJobs.tickTime[strconv.Itoa(tick)] {
			incommingChans.mu.Lock()
			createNewDocToProcess(incommingChans.JChan)
			incommingChans.mu.Unlock()
		}
		tick++
		//fmt.Println("1")
		for jname, _ := range incommingChans.JChan {
			incommingChans.JChan[jname].tickChan <- tick
		}

		if tick%35000 == 0 {
			stats.mu.Lock()
			stats.jobsDone = 0
			stats.seconds = 0
			stats.mu.Unlock()
		}
	}
}

type statsStruct struct {
	mu       sync.RWMutex
	seconds  int
	jobsDone int
}

var stats = statsStruct{}

func jobFinishF(jobFinish chan job, incommingJobsChann commingJobs, config MainStruct) {
	for job := range jobFinish {
		incommingJobsChann.mu.Lock()
		incommingJobsChan := incommingJobsChann.JChan

		if config[job.jobType].ScalerSim.NextStage == "Finish" {

			docsNeedToBeDone.mu.RLock()
			if docsNeedToBeDone.progress[job.MyData.id].done == docsNeedToBeDone.progress[job.MyData.id].haveToBeDone {
				delete(docsNeedToBeDone.progress, job.MyData.id)
				stats.mu.Lock()
				stats.seconds += tick/10 - job.startedAt/10
				stats.jobsDone++
				stats.mu.Unlock()
			}
			docsNeedToBeDone.mu.RUnlock()
		} else {
			//	fmt.Println(incommingJobsChan[config[job.jobType].ScalerSim.NextStage])
			incommingJobsChan[config[job.jobType].ScalerSim.NextStage].jobChan <- incomingJob{name: config[job.jobType].ScalerSim.NextStage, time: job.myTicks, startAt: job.startedAt, myData: job.MyData}
		}
		incommingJobsChann.JChan = incommingJobsChan
		incommingJobsChann.mu.Unlock()
	}
}
