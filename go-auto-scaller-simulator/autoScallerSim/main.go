package autoScallerSim

//  10 ticks == 1 second
import (
	"fmt"
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
	mu           sync.Mutex
	haveToBeDone int
	ticksTaken   int
	done         int
}

type SafeCounter struct {
	mu       sync.RWMutex
	progress map[string]progress
}

var docsNeedToBeDone = SafeCounter{progress: make(map[string]progress)}

func createNewDocToProcess(chanel map[string]deploymentStruct) {
	doc := document{
		pages:  generateRandomNumber(1, 3),
		images: 0,
		id:     uuid.New().String(),
	}
	if generateRandomBool(1, 4) {
		doc.images = generateRandomNumber(1, 5)
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

type WorkerConfig struct {
	TimetilDieRange [2]int
	TimeTilStart    [2]int
	Price           float64
}

var tick = 1

func receiveJobs(Pod *deploymentStruct, incomingJobChan chan incomingJob, name string, Config Worker) {
	for jobID := range incomingJobChan {
		uuid := uuid.New()
		Pod.mu.Lock()
		Pod.jobs[uuid.String()] = job{
			startedAt:   jobID.startAt,
			powerNeeded: generateRandomNumber(Config.PowerNeededMin[0], Config.PowerNeededMin[1]),
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

func Deployment(deployment *deploymentStruct, finishjob chan job, Config Worker, tickChan *chan *Workers) {
	workers := NewWorkers(deployment, finishjob, Config)
	myStats := statsStruct{}
	go receiveJobs(deployment, deployment.jobChan, deployment.podType, Config)
	fmt.Println(deployment.podType, "is ready")

	for tick := range deployment.tickChan {
		// every 5 real seconds do a auto scaller processing tick.
		if tick%50 == 0 {
			*tickChan <- &workers
			<-workers.Verify
		}
		deployment.mu.Lock()
		if stats.seconds != 0 && myStats.seconds != 0 && myStats.jobsDone != 0 && stats.jobsDone != 0 {
			meantimeTotal := stats.seconds / stats.jobsDone
			workers.MyChart.Mu.Lock()
			workers.MyChart.TotalSeconds = append(workers.MyChart.TotalSeconds, int32(meantimeTotal))
			workers.MyChart.Mu.Unlock()
			meantime := myStats.seconds / 10 / myStats.jobsDone / 10
			workers.MyChart.Seconds = append(workers.MyChart.Seconds, int32(meantime))
			myStats.jobsDone = 0
			myStats.seconds = 0
			fmt.Println("View: ", ":8081/", deployment.podType, "| number of jobs:", strconv.Itoa(len(deployment.jobs)), "| number of workers:", len(deployment.workers), "| deployment:", deployment.podType, "| job done mean time:", meantime, "| overall job done time:", meantimeTotal)
		}
		for id, job := range deployment.jobs {
			job.myTicks += 1
			deployment.jobs[id] = job
		}
		for Wid, worker := range deployment.workers {

			if worker.timeTilStart > 0 {
				//fmt.Println(worker.timeTilStart)
				deployment.workers[Wid].timeTilStart -= 1
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
				if myjob.powerNeeded > 0 {
					myjob.powerNeeded -= worker.power
					deployment.jobs[worker.workingon] = myjob
				} else {

					docsNeedToBeDone.mu.Lock()
					doc := docsNeedToBeDone.progress[myjob.MyData.id]
					doc.done += 1
					doc.ticksTaken += tick/10 - deployment.jobs[worker.workingon].startedAt/10
					myStats.jobsDone++
					myStats.seconds += myjob.myTicks / 10
					docsNeedToBeDone.progress[myjob.MyData.id] = doc
					docsNeedToBeDone.mu.Unlock()

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
		jobs:     make(map[string]job),
		podType:  name,
		jobChan:  make(chan incomingJob, 6400),
		tickChan: make(chan int, 5), // change buffer size to change the allowed number of tick desyncs (makes it work faster, 5-40 should be a reasonable amount)
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

func Start(tickChannel *chan *Workers, config MainStruct) {
	incommingChans := commingJobs{
		JChan: make(map[string]deploymentStruct),
	}

	jobFinish := make(chan job)
	for index, _ := range config {
		go Deployment(makeDepoloyment(index, &incommingChans), jobFinish, config[index], tickChannel)
	}

	tikTimingJobs := getTickJobTimings()

	go jobFinishF(jobFinish, &incommingChans, config)
	for {

		if tikTimingJobs.tickTime[strconv.Itoa(tick)] {
			incommingChans.mu.Lock()
			createNewDocToProcess(incommingChans.JChan)
			incommingChans.mu.Unlock()
		}
		tick++
		for jname, _ := range incommingChans.JChan {
			incommingChans.JChan[jname].tickChan <- tick
		}

		if tick%1000 == 0 {
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

func jobFinishF(jobFinish chan job, incommingJobsChann *commingJobs, config MainStruct) {
	for job := range jobFinish {
		incommingJobsChann.mu.Lock()
		incommingJobsChan := incommingJobsChann.JChan

		if config[job.jobType].ScalerSim.NextStage == "Finish" {

			docsNeedToBeDone.mu.RLock()
			if docsNeedToBeDone.progress[job.MyData.id].done == docsNeedToBeDone.progress[job.MyData.id].haveToBeDone && docsNeedToBeDone.progress[job.MyData.id].haveToBeDone > 0 {
				stats.mu.Lock()
				stats.seconds += docsNeedToBeDone.progress[job.MyData.id].ticksTaken / docsNeedToBeDone.progress[job.MyData.id].done
				stats.jobsDone++
				stats.mu.Unlock()
				delete(docsNeedToBeDone.progress, job.MyData.id)
			}
			docsNeedToBeDone.mu.RUnlock()
		} else {
			incommingJobsChan[config[job.jobType].ScalerSim.NextStage].jobChan <- incomingJob{name: config[job.jobType].ScalerSim.NextStage, time: job.myTicks, startAt: job.startedAt, myData: job.MyData}
		}
		incommingJobsChann.JChan = incommingJobsChan
		incommingJobsChann.mu.Unlock()
	}
}
