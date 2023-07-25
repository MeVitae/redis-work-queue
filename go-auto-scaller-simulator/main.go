package main

//Person and signature detection: ~8s each (PSD)

//Section detection: ~5s (SD)

//Reading order: average 300ms, but it can be as high as 50 seconds in rare cases (RO)

//NER: ~35s (NER)

//Feature extraction: ~250ms

// Page of a PDF

// So each entry represents a document, each document has on average 1.8 pages.
// And 2.3 images
// Each image gets a job in the person and signature detection queue.
// Each page gets a job in the section detection queue
// After section detection, there are an average of 6.7 sections. Each section gets a job in the reading order queue.
// After reading order, each of those jobs goes to NER, then feature extraction.

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
)

type document struct {
	pages  int
	images int
	id     string
}

type progress struct {
	mu           sync.Mutex
	haveToBeDone int
	done         int
}

type SafeCounter struct {
	mu       sync.RWMutex
	progress map[string]*progress
}

var docsNeedToBeDone = SafeCounter{progress: make(map[string]*progress)}

func createNewDocToProcess(chanel chan incomingJob, chanelSD chan incomingJob) {
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
	docsNeedToBeDone.progress[doc.id] = &progress{}
	docsNeedToBeDone.mu.Unlock()

	for i := 0; i < doc.images; i++ {
		chanel <- incomingJob{name: "PSD", time: 0, startAt: tick, myData: doc}
	}
	for i := 0; i < doc.pages; i++ {
		chanelSD <- incomingJob{name: "SD", time: 0, startAt: tick, myData: doc}
	}
	//fmt.Println("job")
	//	fmt.Println(doc.images, "-")
	//fmt.Println(docsNeedToBeDone[jobID.myData.id].done)
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
}

// TODO: rename to deployment
type pod struct {
	workers []worker
	jobs    map[string]job
	podType string
}

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
}

var ConfigWorker = map[string]workerConfig{
	"Base": {
		timetilDieRange: [2]int{-1, -1},
		timeTilStart:    [2]int{2200, 2400},
		price:           1,
	},
	"Spot": {
		timetilDieRange: [2]int{130000, 290000},
		timeTilStart:    [2]int{2200, 2400},
		price:           0.15,
	},
	"Fast": {
		timetilDieRange: [2]int{-1, -1},
		timeTilStart:    [2]int{2200, 2400},
		price:           1.2,
	},
}

type workersCount struct {
	workers map[string]int32
}

var tick = 1
var dn = 0
var hvtobedone = 0

func getWorkers(pod pod) (realWorkers map[string]int32, totalWorkers map[string]int32) {
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

func podF(name string, incomingJobChan chan incomingJob, finishjob chan job, inctick chan bool) {
	Pod := pod{
		workers: []worker{},
		jobs:    make(map[string]job),
		podType: name,
	}

	workers := Workers{
		baseName: "base",
		fastName: "fast",
		spotName: "spot",
		queue:    &map[string]int32{},
		calculator: Calculator{
			Target:     50,
			SpotTarget: 50,
			Run:        50,
			Spinup:     120,
		},
		currentSlowDown:     0,
		consecutiveSlowDown: 50,
		currentUP:           0,
		consecutiveUP:       15,
		slowdown:            NewSlowDown(15),
		maxFast:             1500,
	}

	toDropWorkers := make(map[string]int32)
	fmt.Println(name, "is ready")
	//Pod.workers = append(Pod.workers, worker{timetilDie: -1, power: 1, workingon: "", timeTilStart: 140000})
	for {
		select {
		case jobID, ok := <-incomingJobChan:

			if !ok {
				// incomingJobChan is closed, exit the goroutine
				fmt.Println("tickclosed")
				break
			}
			uuid := uuid.New()

			Pod.jobs[uuid.String()] = job{
				startedAt:   jobID.startAt,
				powerNeeded: generateRandomNumber(Config[name].PowerNeededRange[0], Config[name].PowerNeededRange[1]),
				taken:       false,
				myTicks:     jobID.time,
				jobType:     name,
				MyData:      jobID.myData,
			}
			dn += 1

			docsNeedToBeDone.mu.RLock()
			mdoc := docsNeedToBeDone.progress[jobID.myData.id]
			docsNeedToBeDone.mu.RUnlock()
			mdoc.mu.Lock()
			mdoc.haveToBeDone += 1
			mdoc.mu.Unlock()

		case ticking, ok := <-inctick:
			if !ok {
				// incomingJobChan is closed, exit the goroutine
				fmt.Println("tickclosed")
				break
			}
			if ticking {
				if tick%50 == 0 {
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

					for _, worker := range Pod.workers {
						if worker.timeTilStart == 0 {
							realWorkers[worker.wtype]++
						}
						totalWorkers[worker.wtype]++
						totalWorkers2[worker.wtype]++
					}
					newc := workers.Tick(totalWorkers, int32(len(Pod.jobs)), realWorkers)

					for name, workers := range newc {

						//fmt.Println("Pod Workers", Pod.workers)
						//fmt.Println("New workers:", workers, "Acctual workers:", totalWorkers2[name])
						if workers > totalWorkers2[name] {
							DiffenceInWorkers := workers - totalWorkers2[name]
							//	fmt.Println("Creating ", DiffenceInWorkers, " workers of type", name)

							for i := int32(1); i <= DiffenceInWorkers; i++ {

								Pod.workers = append(Pod.workers, worker{timetilDie: generateRandomNumber(ConfigWorker[name].timetilDieRange[0], ConfigWorker[name].timetilDieRange[1]), power: 1, workingon: "", timeTilStart: generateRandomNumber(ConfigWorker[name].timeTilStart[0], ConfigWorker[name].timeTilStart[1]), wtype: name})
								//fmt.Println(Pod.workers)
							}
						} else if workers < totalWorkers2[name] {
							// dropping extra workers
							numberOfDrops := totalWorkers[name] - workers
							toDropWorkers[name] = numberOfDrops

							//		fmt.Println("Have to drop", toDropWorkers, name, "workers")
						}
					}
					writeToFile("workers"+name, strconv.Itoa(len(Pod.workers)))
					writeToFile("readyworkers"+name, strconv.Itoa(int(totalWorkers2["Fast"]+totalWorkers2["Base"]+totalWorkers2["Spot"])))
					writeToFile("jobs"+name, strconv.Itoa(len(Pod.jobs)))
				}

				for id, job := range Pod.jobs {
					job.myTicks += 1
					Pod.jobs[id] = job
				}

				for Wid, worker := range Pod.workers {
					//	fmt.Println(worker.timeTilStart)
					if toDropWorkers[worker.wtype] > 0 && worker.workingon == "" {
						if Wid >= 0 && Wid < len(Pod.workers) {
							Pod.workers = append(Pod.workers[:Wid], Pod.workers[Wid+1:]...)
						}
						continue
					}

					if worker.timeTilStart > 0 {
						worker.timeTilStart -= 1
						//fmt.Println(worker.timeTilStart)
						Pod.workers[Wid] = worker
					} else if worker.workingon == "" {

						for id, job := range Pod.jobs {
							if !job.taken {
								job.taken = true
								Pod.jobs[id] = job
								Pod.workers[Wid].workingon = id
								break
							}
						}

					} else {
						myjob := Pod.jobs[worker.workingon]
						if myjob.powerNeeded%100 == 0 {
							//fmt.Println(myjob.powerNeeded)
						}
						if myjob.powerNeeded > 0 {
							myjob.powerNeeded -= worker.power
							Pod.jobs[worker.workingon] = myjob
						} else {
							docsNeedToBeDone.mu.RLock()
							mdoc := docsNeedToBeDone.progress[myjob.MyData.id]
							docsNeedToBeDone.mu.RUnlock()
							mdoc.mu.Lock()
							mdoc.done += 1
							mdoc.mu.Unlock()

							finishjob <- Pod.jobs[worker.workingon]
							delete(Pod.jobs, worker.workingon)
							Pod.workers[Wid].workingon = ""
						}
					}
				}

			}

		default:
		}

	}
}

func main() {
	go startPlotGraph()
	// Person and signature detection (TODO: split up)
	incomingJobPSD := make(chan incomingJob, 32000)
	// Section detection
	incomingJobSD := make(chan incomingJob, 32000)
	// Reading order
	incomingJobRO := make(chan incomingJob, 32000)
	// NER
	incomingJobNER := make(chan incomingJob, 32000)
	// TODO: add feature extraction

	ticking1 := make(chan bool, 16000)
	ticking2 := make(chan bool, 16000)
	ticking3 := make(chan bool, 16000)
	ticking4 := make(chan bool, 16000)
	jobFinish := make(chan job, 16000)

	fmt.Println("Spawning")
	go podF("PSD", incomingJobPSD, jobFinish, ticking1)
	go podF("SD", incomingJobSD, jobFinish, ticking2)
	go podF("RO", incomingJobRO, jobFinish, ticking3)
	go podF("NER", incomingJobNER, jobFinish, ticking4)
	tikTimingJobs := getTickJobTimings()
	//go createNewDocToProcess(incomingJobPSD, incomingJobSD)
	jobsDone := 0
	seconds := 0
	for {

		if tikTimingJobs.tickTime[strconv.Itoa(tick)] {
			go createNewDocToProcess(incomingJobPSD, incomingJobSD)

		}
		tick++
		//fmt.Println("1")
		ticking1 <- true
		//fmt.Println("2")
		ticking2 <- true
		//fmt.Println("3")
		ticking3 <- true
		//fmt.Println("4")
		ticking4 <- true
		//fmt.Println("5")
		time.Sleep(5000 * time.Microsecond)

		select {
		case job := <-jobFinish:
			//fmt.Println(job)
			if job.jobType == "PSD" {
				incomingJobRO <- incomingJob{name: "RO", time: job.myTicks, startAt: job.startedAt, myData: job.MyData}
				//fmt.Println("Createro")
				//fmt.Println("job PSD", job.myTicks/100, job.startedAt/100)
			} else if job.jobType == "SD" {
				//fmt.Println("Createro")
				incomingJobRO <- incomingJob{name: "RO", time: job.myTicks, startAt: job.startedAt, myData: job.MyData}
				incomingJobRO <- incomingJob{name: "RO", time: job.myTicks, startAt: job.startedAt, myData: job.MyData}
				incomingJobRO <- incomingJob{name: "RO", time: job.myTicks, startAt: job.startedAt, myData: job.MyData}
				incomingJobRO <- incomingJob{name: "RO", time: job.myTicks, startAt: job.startedAt, myData: job.MyData}
				incomingJobRO <- incomingJob{name: "RO", time: job.myTicks, startAt: job.startedAt, myData: job.MyData}
				incomingJobRO <- incomingJob{name: "RO", time: job.myTicks, startAt: job.startedAt, myData: job.MyData}
			} else if job.jobType == "RO" {
				incomingJobNER <- incomingJob{name: "NER", time: job.myTicks, startAt: job.startedAt, myData: job.MyData}
			} else if job.jobType == "NER" {
				//fmt.Println(dn, "dn", docsNeedToBeDone.progress[job.MyData.id])
				docsNeedToBeDone.mu.RLock()
				if docsNeedToBeDone.progress[job.MyData.id].done == docsNeedToBeDone.progress[job.MyData.id].haveToBeDone {
					fmt.Println("job job finished:", tick/100-job.startedAt/100, "Seconds")
					seconds += tick/100 - job.startedAt/100
					jobsDone++
				}
				docsNeedToBeDone.mu.RUnlock()
			}
		default:

		}
		if tick%50 == 0 {
			if jobsDone > 1 {
				writeToFile("tickC", strconv.Itoa(tick))
				writeToFile("SecondsToComplete", strconv.Itoa(seconds/jobsDone))
			}
			writeToFile("tick", strconv.Itoa(tick))
			jobsDone = 0
			seconds = 0
		}
	}
}
