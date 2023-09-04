package graphs

import (
	"fmt"
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

type ChartData struct {
	Mu           sync.RWMutex
	Seconds      []int32
	Ticks        []int32
	Jobs         []int32
	Cost         []int32
	ReadyWorkers []int32
	Workers      []int32
	Name         string
	TotalSeconds []int32
	Config       CharDisplayConfing
}

func httpserver(w http.ResponseWriter, _ *http.Request, Cdata *ChartData) {

	Jobs_Seconds := charts.NewLine()
	Jobs_Seconds.SetGlobalOptions(
		charts.WithInitializationOpts(opts.Initialization{Theme: types.ThemeWesteros}),
		charts.WithTitleOpts(opts.Title{
			Title: "Jobs done time",
		}))

	Jobs_Seconds.SetXAxis(Cdata.convertToIntSlice(Cdata.Ticks)).
		AddSeries("Seconds Taken "+Cdata.Name, Cdata.convertToIntSlice(Cdata.Seconds)).
		AddSeries("Total Seconds taken", Cdata.convertToIntSlice(Cdata.TotalSeconds)).
		SetSeriesOptions(charts.WithLineChartOpts(opts.LineChart{Smooth: false}))
	Workers := charts.NewLine()
	Workers.SetXAxis(Cdata.convertToIntSlice(Cdata.Ticks)).
		AddSeries("Workers", Cdata.convertToIntSlice(Cdata.Workers)).
		AddSeries("Ready Workers", Cdata.convertToIntSlice(Cdata.ReadyWorkers)).
		SetSeriesOptions(charts.WithLineChartOpts(opts.LineChart{Smooth: false}))
	Cost := charts.NewLine()
	Cost.SetXAxis(Cdata.convertToIntSliceCost(Cdata.Ticks)).
		AddSeries("Cost", Cdata.convertToIntSliceCost(Cdata.Cost)).
		SetSeriesOptions(charts.WithLineChartOpts(opts.LineChart{Smooth: false}))
	Cost.Render(w)
	TotalTime := charts.NewLine()
	TotalTime.SetXAxis(Cdata.convertToIntSlice(Cdata.Ticks)).
		//AddSeries("Total Seconds", convertToIntSlice(Cdata.totalSeconds)).
		AddSeries("Number of jobs "+Cdata.Name, Cdata.convertToIntSlice(Cdata.Jobs)).
		SetSeriesOptions(charts.WithLineChartOpts(opts.LineChart{Smooth: false}))
	TotalTime.Render(w)
	Workers.Render(w)
	Jobs_Seconds.Render(w)
}

func StartPlotGraph(name string) *ChartData {
	Cdata := ChartData{
		Name: name,
		Config: CharDisplayConfing{
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

func (Cdata *ChartData) convertToIntSlice(data []int32) []opts.LineData {
	numElements := len(data)
	if Cdata.Config.showAllData == false && numElements > int(Cdata.Config.showLastNumberOfElements) {
		numElements = int(Cdata.Config.showLastNumberOfElements)
	}

	lineDataSlice := make([]opts.LineData, numElements)
	for i := 0; i < numElements; i++ {
		index := len(data) - numElements + i
		lineDataSlice[i] = opts.LineData{Value: data[index]}
	}
	return lineDataSlice
}

func (Cdata *ChartData) convertToIntSliceCost(data []int32) []opts.LineData {
	numElements := len(data)
	if Cdata.Config.showAllData == false && numElements > int(50000) {
		numElements = int(50000)
	}

	fmt.Println(data)
	lineDataSlice := make([]opts.LineData, numElements)
	for i := 0; i < numElements; i++ {
		index := len(data) - numElements + i
		lineDataSlice[i] = opts.LineData{Value: data[index]}
	}
	return lineDataSlice
}

type GraphInfo struct {
	Deployment string
	DataType   string
	Data       int32
}
