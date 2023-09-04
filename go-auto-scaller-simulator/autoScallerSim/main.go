package autoScallerSim

//  10 ticks == 1 second
import (
	"fmt"
	"go-auto-scaller-simulator/graphs"
	_ "go-auto-scaller-simulator/graphs"
	"go-auto-scaller-simulator/interfaces"
	"io/ioutil"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"gopkg.in/yaml.v2"
)

type document struct {
	id string
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

type WorkersConfig map[string]WorkerConfig

func GetWorkersConfig(workersFile string) WorkersConfig {
	data, _ := ioutil.ReadFile(workersFile)
	var result WorkersConfig
	err := yaml.Unmarshal([]byte(data), &result)
	if err != nil {
		log.Fatalln(err)
	}

	return result
}

func GetProcessStarterConfig(PSFile string) (result ProcessStarter) {
	data, _ := ioutil.ReadFile(PSFile)
	err := yaml.Unmarshal([]byte(data), &result)
	if err != nil {
		log.Fatalln(err)
	}

	return result
}

var docsNeedToBeDone = SafeCounter{progress: make(map[string]progress)}

func createNewDocToProcess(chanel map[string]deploymentStruct, processStarter ProcessStarterConfig) {
	for _, processToStart := range processStarter {
		doc := document{
			id: uuid.New().String(),
		}

		// Lock so only one goroutine at a time can access the map c.v.
		docsNeedToBeDone.mu.Lock()
		docsNeedToBeDone.progress[doc.id] = progress{}
		docsNeedToBeDone.mu.Unlock()
		toStart := generateRandomNumber(0, processToStart.StartRange)
		for i := 0; i < toStart; i++ {
			chanel[processToStart.StartProcess].jobChan <- incomingJob{name: processToStart.StartProcess, time: 0, startAt: tick, myData: doc}
		}
	}
}

func createNewDocToProcessSpecific(chanel map[string]deploymentStruct, processStarter ProcessStarter) {
	doc := document{
		id: uuid.New().String(),
	}
	// Lock so only one goroutine at a time can access the map c.v.
	docsNeedToBeDone.mu.Lock()
	docsNeedToBeDone.progress[doc.id] = progress{}
	docsNeedToBeDone.mu.Unlock()
	fmt.Println(processStarter)
	chanel[processStarter.StartProcess].jobChan <- incomingJob{name: processStarter.StartProcess, time: 0, startAt: tick, myData: doc}

}

type worker struct {
	timetilDie   int
	timeTilStart int
	power        int
	cost         float32
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
	TimetilDieRange [2]int  `yaml:"TimetilDieRange"`
	TimeTilStart    [2]int  `yaml:"TimeTilStart"`
	Price           float32 `yaml:"Price"`
}

var tick = 12000015

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

func Deployment(deployment *deploymentStruct, finishjob chan job, Config Worker, tickChan *chan *Workers, WorkersConfig map[string]WorkerConfig, graphInfoCH *chan graphs.GraphInfo, sendWorkerBack chan *Workers) {
	workers := NewWorkers(deployment, finishjob, Config, WorkersConfig)
	sendWorkerBack <- &workers
	myStats := statsStruct{}
	meantimeTotal := 0
	meantime := 0
	var cost int32
	go receiveJobs(deployment, deployment.jobChan, deployment.podType, Config)
	fmt.Println(deployment.podType, "is ready")

	for tick := range deployment.tickChan {
		// every 5 real seconds do a auto scaller processing tick.

		if tick%50 == 0 {
			*graphInfoCH <- graphs.GraphInfo{Deployment: deployment.podType, DataType: "Cost", Data: cost}
			if stats.seconds != 0 && myStats.seconds != 0 && myStats.jobsDone != 0 && stats.jobsDone != 0 {
				meantimeTotal = stats.seconds / stats.jobsDone
				*graphInfoCH <- graphs.GraphInfo{Deployment: deployment.podType, DataType: "TotalSeconds", Data: int32(meantimeTotal)}
				meantime = myStats.seconds / 10 / myStats.jobsDone / 10
				*graphInfoCH <- graphs.GraphInfo{Deployment: deployment.podType, DataType: "Seconds", Data: int32(meantime)}
				myStats.jobsDone = 0
				myStats.seconds = 0
				//fmt.Println("View: ", ":8081/", deployment.podType, "| number of jobs:", strconv.Itoa(len(deployment.jobs)), "| number of workers:", len(deployment.workers), "| deployment:", deployment.podType, "| job done mean time:", meantime, "| overall job done time:", meantimeTotal)
			} else {
				*graphInfoCH <- graphs.GraphInfo{Deployment: deployment.podType, DataType: "TotalSeconds", Data: int32(meantimeTotal)}
				*graphInfoCH <- graphs.GraphInfo{Deployment: deployment.podType, DataType: "Seconds", Data: int32(meantime)}

			}
			*tickChan <- &workers
			<-workers.Verify
		}
		deployment.mu.Lock()
		for id, job := range deployment.jobs {
			job.myTicks += 1
			deployment.jobs[id] = job
		}

		cost += int32(deployment.CalculateCost())

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
					myStats.seconds += tick/10 - deployment.jobs[worker.workingon].startedAt/10
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

type SimulatedDeployments map[string]*Workers

func (Deployment SimulatedDeployments) GetDeployment(wId string) interfaces.Deployment {
	splittedString := strings.Split(wId, "/")
	return Deployment[splittedString[0]].GetDeployment(splittedString[1])
}
func Start(tickChannel *chan *Workers, config MainStruct, WorkersConfig map[string]WorkerConfig, GraphInfo *chan graphs.GraphInfo, processStarter ProcessStarterConfig, simulatedDeployments map[string]*Workers) {
	incommingChans := commingJobs{
		JChan: make(map[string]deploymentStruct),
	}
	//fmt.Println(WorkersConfig)
	jobFinish := make(chan job)
	for index, _ := range config {
		deployment := makeDepoloyment(index, &incommingChans)
		receiveWorker := make(chan *Workers)
		go Deployment(deployment, jobFinish, config[index], tickChannel, WorkersConfig, GraphInfo, receiveWorker)
		workerDeployment := <-receiveWorker
		simulatedDeployments[index] = workerDeployment
	}

	tikTimingJobs := getTickJobTimings(false)

	go jobFinishF(jobFinish, &incommingChans, config)
	for {
		time.Sleep(time.Nanosecond * 200)
		if tikTimingJobs.tickTime[strconv.Itoa(tick)] == "true" {
			incommingChans.mu.Lock()
			createNewDocToProcess(incommingChans.JChan, processStarter)
			incommingChans.mu.Unlock()
		} else if tikTimingJobs.tickTime[strconv.Itoa(tick)] != "" {
			incommingChans.mu.Lock()
			fmt.Println(tick)
			toStart := ProcessStarter{StartProcess: tikTimingJobs.tickTime[strconv.Itoa(tick)]}

			createNewDocToProcessSpecific(incommingChans.JChan, toStart)
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

		if config[job.jobType].ScalerSim.ChildWorker == "Finish" {

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
			incommingJobsChan[config[job.jobType].ScalerSim.ChildWorker].jobChan <- incomingJob{name: config[job.jobType].ScalerSim.ChildWorker, time: job.myTicks, startAt: job.startedAt, myData: job.MyData}
		}
		incommingJobsChann.JChan = incommingJobsChan
		incommingJobsChann.mu.Unlock()
	}
}
