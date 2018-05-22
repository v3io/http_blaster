package worker

type WorkerType int32

const (
	PERFORMANCE_WORKER WorkerType = iota
	INGESTION_WORKER   WorkerType = iota
)
