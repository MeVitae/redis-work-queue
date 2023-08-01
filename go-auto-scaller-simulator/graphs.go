package main

import (
	"net/http"

	"github.com/go-echarts/go-echarts/v2/charts"
	"github.com/go-echarts/go-echarts/v2/opts"
	"github.com/go-echarts/go-echarts/v2/types"
)

var jobs = make([]string, 0)

type ChartWorkers struct {
	Fast []int32
	Base []int32
	Spot []int32
}

type chartData struct {
	seconds      []int32
	ticks        []int32
	jobs         []int32
	readyWorkers ChartWorkers
	workers      ChartWorkers
	name         string
	totalSeconds []int32
}

func httpserver(w http.ResponseWriter, _ *http.Request, Cdata *chartData) {

	Jobs_Seconds := charts.NewLine()
	Jobs_Seconds.SetGlobalOptions(
		charts.WithInitializationOpts(opts.Initialization{Theme: types.ThemeWesteros}),
		charts.WithTitleOpts(opts.Title{
			Title: "Jobs done time",
		}))

	Jobs_Seconds.SetXAxis(convertToIntSlice(Cdata.ticks)).
		AddSeries("Number of jobs", convertToIntSlice(Cdata.jobs)).
		AddSeries("Seconds Taken", convertToIntSlice(Cdata.seconds)).
		//AddSeries("Jobs Queue", GetFileDataFromSavedData("JobsQueue")).
		SetSeriesOptions(charts.WithLineChartOpts(opts.LineChart{Smooth: false}))
	RWorkers := charts.NewLine()
	RWorkers.SetXAxis(convertToIntSlice(Cdata.ticks)).
		AddSeries("Ready Base workers", convertToIntSlice(Cdata.readyWorkers.Base)).
		AddSeries("Ready Fast workers", convertToIntSlice(Cdata.readyWorkers.Fast)).
		AddSeries("Ready Spot workers", convertToIntSlice(Cdata.readyWorkers.Spot)).
		//AddSeries("Jobs Queue", GetFileDataFromSavedData("JobsQueue")).
		SetSeriesOptions(charts.WithLineChartOpts(opts.LineChart{Smooth: false}))

	Workers := charts.NewLine()
	Workers.SetXAxis(convertToIntSlice(Cdata.ticks)).
		AddSeries("Base workers", convertToIntSlice(Cdata.workers.Base)).
		AddSeries("Fast workers", convertToIntSlice(Cdata.workers.Fast)).
		AddSeries("Spot workers", convertToIntSlice(Cdata.workers.Spot)).
		//AddSeries("Jobs Queue", GetFileDataFromSavedData("JobsQueue")).
		SetSeriesOptions(charts.WithLineChartOpts(opts.LineChart{Smooth: false}))

	TotalTime := charts.NewLine()
	TotalTime.SetXAxis(convertToIntSlice(Cdata.ticks)).
		AddSeries("Total Seconds", convertToIntSlice(Cdata.totalSeconds)).
		//AddSeries("Jobs Queue", GetFileDataFromSavedData("JobsQueue")).
		SetSeriesOptions(charts.WithLineChartOpts(opts.LineChart{Smooth: false}))
	TotalTime.Render(w)
	Workers.Render(w)
	Jobs_Seconds.Render(w)
	RWorkers.Render(w)
}

func startPlotGraph(name string) *chartData {
	Cdata := chartData{}
	go http.HandleFunc("/"+name, func(w http.ResponseWriter, r *http.Request) {
		httpserver(w, r, &Cdata)
	})
	go http.ListenAndServe(":8081", nil)
	return &Cdata
}

func convertToIntSlice(data []int32) []opts.LineData {
	// Calculate the number of elements to use (either the last 5000 or the full length if it's less than 5000).
	numElements := len(data)
	if numElements > 100 {
		numElements = 100
	}

	lineDataSlice := make([]opts.LineData, numElements)
	// Start from the last 5000 elements and populate the lineDataSlice in reverse order.
	for i := 0; i < numElements; i++ {
		index := len(data) - numElements + i
		lineDataSlice[i] = opts.LineData{Value: data[index]}
	}
	return lineDataSlice
}
