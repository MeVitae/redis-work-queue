module go-auto-scaller-simulator/autoScallerSim

go 1.20

require (
	go-auto-scaller-simulator/graphs v0.0.0-00010101000000-000000000000
	github.com/go-echarts/go-echarts/v2 v2.2.7
	github.com/google/uuid v1.3.0
	gopkg.in/yaml.v2 v2.4.0
)

require github.com/kr/text v0.1.0 // indirect

replace go-auto-scaller-simulator/graphs => ../graphs