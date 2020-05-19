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

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

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
