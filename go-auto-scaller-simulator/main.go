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
package main

import (
	"fmt"
	"time"
)

var jobQ = queue{
	jobs:        []job{},
	workers:     []worker{},
	workingJobs: []currentJob{},
}
var tick = 0
var speedRate = 100
var totalJobsDone = 0

func main() {
	doc := createDoc()
	addJob(&jobQ, doc.myJobs)
	doc2 := createDoc()
	addJob(&jobQ, doc2.myJobs)
	addWorker(createWorker("Base"))
	addWorker(createWorker("Spot"))
	for {
		tick += 1
		// Sleep for 10 milliseconds
		time.Sleep(1 * time.Millisecond)
		performWorkersTick()
	}
}

func performWorkersTick() {
	for i, _ := range jobQ.jobs {
		if jobQ.jobs[i].mySubJobs["PSD"].jobs.availableJobs > 0 {
			for i1 := 0; i1 < jobQ.jobs[i].mySubJobs["PSD"].jobs.availableJobs; i1++ {
				fmt.Println("Iteration:", i1)
				theJob := currentJob{
					time:           jobQ.jobs[i].mySubJobs["PSD"].originalTime,
					CurrentSection: jobQ.jobs[i].mySubJobs["PSD"].nextJob,
					NextSection:    "PSD",
					jobId:          jobQ.jobs[i].id,
				}
				job := jobQ.jobs[i].mySubJobs["PSD"]
				job.jobs.availableJobs = job.jobs.availableJobs - 1
				jobQ.jobs[i].mySubJobs["PSD"] = job

				jobQ.workingJobs = append(jobQ.workingJobs, theJob)
				fmt.Println("Job Added ", job)
			}
		}

	}

	//fmt.Println("Workable processes:", jobQ.workingJobs)
	toDropWorkerId := -1
	for i1, _ := range jobQ.workers {

		worker := jobQ.workers[i1]
		job := jobQ.workers[i1]
		if job.timeTilDrop != -1 {
			if job.timeTilDrop > 0 {
				job.timeTilDrop -= 1
			} else {
				toDropWorkerId = i1
			}

		}

		if worker.myJob == (currentJob{}) && len(jobQ.workingJobs) > 0 {
			jobQ.workers[i1].myJob = jobQ.workingJobs[0]
			jobQ.workingJobs = jobQ.workingJobs[1:]
			totalJobsDone += 1
			//fmt.Println("Free Worker ", i1, ":", worker)
		} else if worker.myJob != (currentJob{}) {

			if job.myJob.time > 0 {
				job.myJob.time -= 1
			} else {
				// create next job;

				// {0 PSD SD d9bafec8-2871-4958-bb58-71b238991478}
				for i1, _ := range jobQ.jobs {
					if jobQ.jobs[i1].id == job.myJob.jobId {
						// getting the original job
						OriginalJob := jobQ.jobs[i1]
						//fmt.Println("Job data:", OriginalJob)
						JobSection := OriginalJob.mySubJobs[job.myJob.NextSection]
						if JobSection.jobs.maxJobs > JobSection.jobs.jobsCreated {
							JobSection.jobs.availableJobs += 1
							JobSection.jobs.jobsCreated += 1
						}
						if job.myJob.NextSection != "FINISHED" {
							theJob := currentJob{
								time:           JobSection.originalTime,
								CurrentSection: job.myJob.NextSection,
								NextSection:    JobSection.nextJob,
								jobId:          job.myJob.jobId,
							}

							jobQ.workingJobs = append(jobQ.workingJobs, theJob)
						}
					}

				}
				//fmt.Println("My data from worker -- ", job.myJob)

				//---

				// move current worker to next job;
				job.myJob = currentJob{}
			}
			// mangaing spot worker timeouts.

			jobQ.workers[i1] = job
		}
	}

	if toDropWorkerId != -1 {
		fmt.Println("Handling job drop:", jobQ.workers[toDropWorkerId])

		//move job back to queue
		jobQ.workingJobs = append(jobQ.workingJobs, jobQ.workers[toDropWorkerId].myJob)
		//drop worker
		jobQ.workers = append(jobQ.workers[:toDropWorkerId], jobQ.workers[toDropWorkerId+1:]...)

	}

	if tick%111 == 0 {
		fmt.Println("Workers:", jobQ.workers)
		fmt.Println("Working Jobs:", jobQ.workingJobs)
		//fmt.Println("Tick:", tick)
		//fmt.Println("JobsDone", totalJobsDone)
	}
}

//job queue should be implemented diffrently

/// current workers less or equal to number of jobs left per jobsection then can take job.

// can do subjob description as 1 item is finished from 1 side move it to can do to the other side in case no jobs available there will be jobs as well as its more realistic
