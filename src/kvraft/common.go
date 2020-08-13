package kvraft

import (
	"math/rand"
	"strings"
	"time"
)

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

// // a helper struct that helps the data comprehension during snapshot
// type SnapshotComplex struct {
// 	KeyValueMap    map[string]string
// 	IdentifiersMap map[string]bool
// }

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	// clerk ID
	ID string

	// unique identifiers
	Identifier string
}

type PutAppendReply struct {
	Err Err
	// Success  bool
	// isLeader bool
}

type GetArgs struct {
	Key string

	// You'll have to add definitions here.

	// clerk ID
	ID string

	// unique identifiers
	Identifier string
}

type GetReply struct {
	Err Err
	// Success  bool
	// isLeader bool
	Value string
}

// helper function that generates a random string, input the length of string
func GenerateRandomString(length int) (str string) {
	rand.Seed(time.Now().UnixNano())
	chars := []rune("ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ" +
		"abcdefghijklmnopqrstuvwxyzåäö" +
		"0123456789")
	var b strings.Builder
	for i := 0; i < length; i++ {
		b.WriteRune(chars[rand.Intn(len(chars))])
	}
	str = b.String() // E.g. "ExcbsVQs"
	return
}
