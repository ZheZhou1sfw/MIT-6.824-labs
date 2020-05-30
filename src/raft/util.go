package raft

import "log"

// Debugging
const Debug = 0

// a helper struct that helps the data comprehension during snapshot
type SnapshotComplex struct {
	IdentifiersMap map[string]bool
	KeyValueMap    map[string]string
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}
