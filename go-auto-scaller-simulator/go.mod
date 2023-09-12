module main

go 1.21.1

replace go-auto-scaller-simulator/graphs => ./graphs

replace go-auto-scaller-simulator/autoScallerSim => ./autoScallerSim

replace github.com/mevitae/redis-work-queue/autoscale/interfaces => ./../autoscale/interfaces

replace github.com/mevitae/redis-work-queue/autoscale/wqautoscale => ./../autoscale/wqautoscale

require (
	github.com/mevitae/redis-work-queue/autoscale/wqautoscale v0.0.0-00010101000000-000000000000
	go-auto-scaller-simulator/autoScallerSim v0.0.0-00010101000000-000000000000
	go-auto-scaller-simulator/graphs v0.0.0-00010101000000-000000000000
)

require (
	github.com/go-echarts/go-echarts/v2 v2.2.7 // indirect
	github.com/google/uuid v1.3.0 // indirect
	github.com/mevitae/redis-work-queue/autoscale/interfaces v0.0.0-00010101000000-000000000000 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
)
