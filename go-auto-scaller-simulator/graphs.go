package main

import (
	"net/http"

	"github.com/go-echarts/go-echarts/v2/charts"
	"github.com/go-echarts/go-echarts/v2/opts"
	"github.com/go-echarts/go-echarts/v2/types"
)

var jobs = make([]string, 0)

func httpserver(w http.ResponseWriter, _ *http.Request) {

	jobs = append(jobs, "PSD")
	jobs = append(jobs, "SD")
	jobs = append(jobs, "RO")
	jobs = append(jobs, "NER")

	// create a new line instance
	line := charts.NewLine()
	// set some global options like Title/Legend/ToolTip or anything else

	line.SetGlobalOptions(
		charts.WithInitializationOpts(opts.Initialization{Theme: types.ThemeWesteros}),
		charts.WithTitleOpts(opts.Title{
			Title: "Jobs done time",
		}))
	// Put data into instance

	line.SetXAxis(GetFileDataFromSavedData("tickC")).
		AddSeries("Jobs Done Time", GetFileDataFromSavedData("SecondsToComplete")).
		//AddSeries("Jobs Queue", GetFileDataFromSavedData("JobsQueue")).
		SetSeriesOptions(charts.WithLineChartOpts(opts.LineChart{Smooth: false}))
	line.Render(w)

	AverageJobDoneTime := charts.NewLine()
	// set some global options like Title/Legend/ToolTip or anything else

	AverageJobDoneTime.SetGlobalOptions(
		charts.WithInitializationOpts(opts.Initialization{Theme: types.ThemeWesteros}),
		charts.WithTitleOpts(opts.Title{
			Title: "Jobs",
		}))
	// Put data into instance

	AverageJobDoneTime.SetXAxis(GetFileDataFromSavedData("tick")).
		AddSeries("jobs NER", GetFileDataFromSavedData("jobsNER")).
		AddSeries("jobs PSD", GetFileDataFromSavedData("jobsPSD")).
		AddSeries("jobs RO", GetFileDataFromSavedData("jobsRO")).
		AddSeries("jobs NER", GetFileDataFromSavedData("jobsNER")).
		//AddSeries("Jobs Queue", GetFileDataFromSavedData("JobsQueue")).
		SetSeriesOptions(charts.WithLineChartOpts(opts.LineChart{Smooth: false}))
	AverageJobDoneTime.Render(w)

	// create a new line instance
	line2 := charts.NewLine()
	// set some global options like Title/Legend/ToolTip or anything else
	line2.SetGlobalOptions(
		charts.WithInitializationOpts(opts.Initialization{Theme: types.ThemeWesteros}))
	// Put data into instance
	line2.SetXAxis(GetFileDataFromSavedData("tick")).
		AddSeries("Workers PSD", GetFileDataFromSavedData("workersPSD")).
		AddSeries("Workers RO", GetFileDataFromSavedData("workersRO")).
		AddSeries("Workers NER", GetFileDataFromSavedData("workersNER")).
		AddSeries("Ready workers NER", GetFileDataFromSavedData("readyworkersNER")).
		AddSeries("Ready workers  PSD", GetFileDataFromSavedData("readyworkersPSD")).
		AddSeries("Ready workers  RO", GetFileDataFromSavedData("readyworkersRO")).
		SetSeriesOptions(charts.WithLineChartOpts(opts.LineChart{Smooth: false}))
	line2.Render(w)

}

func startPlotGraph() {
	http.HandleFunc("/", httpserver)
	http.ListenAndServe(":8081", nil)
}
