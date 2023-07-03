package main

import (
	"fmt"

	"github.com/google/uuid"
)

type queue struct {
	workingJobs []currentJob
	jobs        []job
	workers     []worker
}

type currentJob struct {
	time           float32
	NextSection    string
	CurrentSection string
	jobId          string
}

type jobQueue struct {
	job                    document
	timeInQueue            int
	timesToBeCompletedLeft int
}

type JobConfig struct {
	timeRange        [2]int
	NrOfSubJobsRange [2]int
	nextJob          string
}

type JobsLeft struct {
	availableJobs  int
	maxJobs        int
	currentWorkers int
	jobsCreated    int
}

type JobSection struct {
	originalTime float32
	nextJob      string
	jobs         JobsLeft // How many times times this job has to be done
}

type job struct {
	currentJob string
	mySubJobs  map[string]JobSection
	hasWorker  bool
	id         string
}

type JobDesc struct {
	numberOfSubJobsRange [2]int
}

func createJob() job {
	pages := generateRandomNumber(1, 3)
	images := generateRandomNumber(1, 4)
	job := job{
		id: uuid.New().String(),
		mySubJobs: map[string]JobSection{
			"PSD": JobSection{
				originalTime: float32(generateRandomNumber(JobQueueing["PSD"].timeRange[0], JobQueueing["PSD"].timeRange[1])),
				nextJob:      JobQueueing["PSD"].nextJob,
				jobs: JobsLeft{
					availableJobs:  images,
					maxJobs:        images,
					currentWorkers: 0,
					jobsCreated:    0,
				},
			},
			"SD": JobSection{
				originalTime: float32(generateRandomNumber(JobQueueing["SD"].timeRange[0], JobQueueing["SD"].timeRange[1])),
				nextJob:      JobQueueing["SD"].nextJob,
				jobs: JobsLeft{
					availableJobs:  0,
					maxJobs:        pages,
					currentWorkers: 0,
					jobsCreated:    0,
				},
			},
			"RO": JobSection{
				originalTime: float32(generateRandomNumber(JobQueueing["RO"].timeRange[0], JobQueueing["RO"].timeRange[1])),
				nextJob:      JobQueueing["RO"].nextJob,
				jobs: JobsLeft{
					availableJobs:  0,
					maxJobs:        pages * 7,
					currentWorkers: 0,
					jobsCreated:    0,
				},
			},
			"NER": JobSection{
				originalTime: float32(generateRandomNumber(JobQueueing["NER"].timeRange[0], JobQueueing["NER"].timeRange[1])),
				nextJob:      JobQueueing["NER"].nextJob,
				jobs: JobsLeft{
					availableJobs:  0,
					maxJobs:        images + pages + (pages * 7),
					currentWorkers: 0,
					jobsCreated:    0,
				},
			},
			"FE": JobSection{
				originalTime: float32(generateRandomNumber(JobQueueing["FE"].timeRange[0], JobQueueing["FE"].timeRange[1])),
				nextJob:      "FINISHED",
				jobs: JobsLeft{
					availableJobs:  0,
					maxJobs:        images + pages + (pages * 7),
					currentWorkers: 0,
					jobsCreated:    0,
				},
			},
		},
	}

	return job
}

func addJob(jobQ *queue, Jobtoadd job) {
	jobQ.jobs = append(jobQ.jobs, Jobtoadd)
	fmt.Printf("Added Job: %s\n", Jobtoadd.currentJob)
}

var JobQueueing = map[string]JobConfig{
	"PSD": JobConfig{
		timeRange:        [2]int{6000, 9000},
		nextJob:          "SD",
		NrOfSubJobsRange: [2]int{1, 3},
	},
	"SD": JobConfig{
		timeRange:        [2]int{2500, 7500},
		nextJob:          "RO",
		NrOfSubJobsRange: [2]int{5, 8},
	},
	"RO": JobConfig{
		timeRange:        [2]int{300, 500},
		nextJob:          "NER",
		NrOfSubJobsRange: [2]int{5, 8},
	},
	"NER": JobConfig{
		timeRange:        [2]int{25000, 45000},
		nextJob:          "FE",
		NrOfSubJobsRange: [2]int{1, 5},
	},
	"FE": JobConfig{
		timeRange:        [2]int{200, 300},
		nextJob:          "Finished",
		NrOfSubJobsRange: [2]int{1, 5},
	},
}
