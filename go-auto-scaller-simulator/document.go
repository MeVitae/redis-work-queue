package main

import "fmt"

type document struct {
	pages   int
	images  int
	myJobs  job
	jobTime int
}
type documentConfig struct {
	pages       int
	pageRange   [2]int
	images      int
	imagesRange [2]int
	myJobs      job
	jobTime     int
}

var documentIn = documentConfig{
	pages:       0,
	pageRange:   [2]int{1, 3},
	images:      0,
	imagesRange: [2]int{1, 4},
	jobTime:     0,
}

func createDoc() document {
	doc := document{}
	initialJob := JobQueueing["PSD"]
	doc.myJobs = createJob()
	doc.pages = generateRandomNumber(documentIn.pageRange[0], documentIn.pageRange[1])
	doc.images = generateRandomNumber(initialJob.NrOfSubJobsRange[0], initialJob.NrOfSubJobsRange[1])
	fmt.Println(doc, "asd")
	return doc
}
