package tests

import (
	"os"
)

const (
	runningEnv = "RUNNING_ENV"
)

func isLocalEnv() bool {
	return os.Getenv(runningEnv) == "LOCAL"
}
