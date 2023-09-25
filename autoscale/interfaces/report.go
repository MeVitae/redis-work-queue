package interfaces

type ScaleReport struct {
    // Job is the name of job (i.e. the work queue name)
	Job                   string
    // Time is a monotonic time value, determined by the autoscaler.
	Time                  int64
    // QLen is the length of the work queue, not including items being processed.
	QLen                  int32
    // Processing is the number of items being processed.
	Processing            int32
    // TotalReadyWorkers is the total ready count of all tiers.
	TotalReadyWorkers     int32
    // TotalReadyWorkers is the total requested count of all tiers.
	TotalRequestedWorkers int32
    // Tiers is the scale for the individual deployment tiers used.
	Tiers                 []ScaleReportTier
}

type ScaleReportTier struct {
	DeploymentName string
	Requested      int32
	Ready          int32
}

// ScaleReporter is the interface used by autoscalers to report information about their scaling.
type ScaleReporter interface {
	// ReportScale should be called every time the autoscaler checks or updates
	// any scale or queue (unless the ScaleReporter is nil).
	ReportScale(ScaleReport)
}
