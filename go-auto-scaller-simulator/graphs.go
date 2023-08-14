package main

import (
	"net/http"
	"sync"

	"github.com/go-echarts/go-echarts/v2/charts"
	"github.com/go-echarts/go-echarts/v2/opts"
	"github.com/go-echarts/go-echarts/v2/types"
)

type CharDisplayConfing struct {
	showAllData              bool
	showLastNumberOfElements int32
}

type chartData struct {
	mu           sync.RWMutex
	seconds      []int32
	ticks        []int32
	jobs         []int32
	readyWorkers []int32
	workers      []int32
	name         string
	totalSeconds []int32
	config       CharDisplayConfing
}

func httpserver(w http.ResponseWriter, _ *http.Request, Cdata *chartData) {

	Jobs_Seconds := charts.NewLine()
	Jobs_Seconds.SetGlobalOptions(
		charts.WithInitializationOpts(opts.Initialization{Theme: types.ThemeWesteros}),
		charts.WithTitleOpts(opts.Title{
			Title: "Jobs done time",
		}))

	Jobs_Seconds.SetXAxis(Cdata.convertToIntSlice(Cdata.ticks)).
		AddSeries("Seconds Taken "+Cdata.name, Cdata.convertToIntSlice(Cdata.seconds)).
		AddSeries("Total Seconds taken", Cdata.convertToIntSlice(Cdata.totalSeconds)).
		SetSeriesOptions(charts.WithLineChartOpts(opts.LineChart{Smooth: false}))
	Workers := charts.NewLine()
	Workers.SetXAxis(Cdata.convertToIntSlice(Cdata.ticks)).
		AddSeries("Workers", Cdata.convertToIntSlice(Cdata.workers)).
		AddSeries("Ready Workers", Cdata.convertToIntSlice(Cdata.readyWorkers)).

		//AddSeries("Jobs Queue", GetFileDataFromSavedData("JobsQueue")).
		SetSeriesOptions(charts.WithLineChartOpts(opts.LineChart{Smooth: false}))

	TotalTime := charts.NewLine()
	TotalTime.SetXAxis(Cdata.convertToIntSlice(Cdata.ticks)).
		//AddSeries("Total Seconds", convertToIntSlice(Cdata.totalSeconds)).
		AddSeries("Number of jobs "+Cdata.name, Cdata.convertToIntSlice(Cdata.jobs)).
		SetSeriesOptions(charts.WithLineChartOpts(opts.LineChart{Smooth: false}))
	TotalTime.Render(w)
	Workers.Render(w)
	Jobs_Seconds.Render(w)
}

func startPlotGraph(name string) *chartData {
	Cdata := chartData{
		name: name,
		config: CharDisplayConfing{
			showAllData:              false,
			showLastNumberOfElements: 1000,
		},
	}
	go http.HandleFunc("/"+name, func(w http.ResponseWriter, r *http.Request) {
		httpserver(w, r, &Cdata)
	})
	go http.ListenAndServe(":8081", nil)
	return &Cdata
}

func (Cdata *chartData) convertToIntSlice(data []int32) []opts.LineData {
	numElements := len(data)
	if Cdata.config.showAllData == false && numElements > int(Cdata.config.showLastNumberOfElements) {
		numElements = int(Cdata.config.showLastNumberOfElements)
	}

	lineDataSlice := make([]opts.LineData, numElements)
	for i := 0; i < numElements; i++ {
		index := len(data) - numElements + i
		lineDataSlice[i] = opts.LineData{Value: data[index]}
	}
	return lineDataSlice
}
