module github.com/mevitae/redis-work-queue/tests/go

replace github.com/mevitae/redis-work-queue/go => ../../go

go 1.20

require (
	github.com/mevitae/redis-work-queue/go v0.1.0
	github.com/redis/go-redis/v9 v9.0.2
)

require (
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/google/uuid v1.3.0 // indirect
)
