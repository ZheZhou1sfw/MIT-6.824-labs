package mr

import (
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Master struct {
	// Your definitions here.
	mapJobs    []MrJob
	reduceJobs []MrJob

	runningMapJobs        []*MrJob
	runningMapJobsTimeMap map[*MrJob]time.Time

	reduceGenerated          bool
	runningReduceJobs        []*MrJob
	runningReduceJobsTimeMap map[*MrJob]time.Time

	nReduce    int
	hashKeyMap map[string]bool

	mux sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) GetAJob(req RPCrequest, res *RPCresponse) (err error) {
	// -------Debug Info -------- //
	// fmt.Println("want a job")
	// fmt.Println(m.mapJobs)
	// fmt.Println(m.runningMapJobs)
	// fmt.Println(m.reduceJobs)
	// fmt.Println(m.runningReduceJobs)
	// fmt.Println(m.hashKeyMap)
	// fmt.Println("-----------")

	// check if any pending job is over due
	m.checkPendingJobs()

	var toDeliver *MrJob
	if m.Done() {
		// err = errors.New("done")
		toDeliver = createExitJob()
	} else if len(m.mapJobs)+len(m.runningMapJobs) > 0 {
		// deliver a map job if there is any map job left
		if len(m.mapJobs) > 0 {
			m.mux.Lock()
			toDeliver = createMapJob(m)
			m.mux.Unlock()
		} else {
			toDeliver = createOnHoldJob()
		}
	} else {
		// if it's the first time map finishes, we need to generate all reduce jobs
		if !m.reduceGenerated {
			m.mux.Lock()
			generateReduceJobs(m)
			m.mux.Unlock()
			m.reduceGenerated = true
		}

		// deliver a reduce job if there is any reduce job left
		if len(m.reduceJobs) > 0 {
			m.mux.Lock()
			toDeliver = createReduceJob(m)
			m.mux.Unlock()
		} else {
			toDeliver = createOnHoldJob()
		}

	}
	res.CurJob = toDeliver
	// fmt.Println(toDeliver)
	return // err is nil by default
}

// notify master one a worker finished a job
func (m *Master) NotifyFinish(job *MrJob, res *NotifyResponse) (err error) {
	// check if it's a outdated worker
	if _, ok := m.hashKeyMap[job.Hashkey]; !ok {
		res.Ack = false
		return
	}

	m.mux.Lock()
	if job.JobType == "map" {
		m.runningMapJobs = deleteAndReslice(m.runningMapJobs, job.FileName)
		newReduceJob := *job // make a copy
		newReduceJob.JobType = "reduce"
		m.reduceJobs = append(m.reduceJobs, newReduceJob)
	} else if job.JobType == "reduce" {
		m.runningReduceJobs = deleteAndReslice(m.runningReduceJobs, job.FileName)
	} else {
		log.Fatal("Cannot notifyFinish this job type" + job.JobType)
	}

	delete(m.hashKeyMap, job.Hashkey)
	m.mux.Unlock()

	res.Ack = true

	return
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	if len(m.mapJobs)+len(m.runningMapJobs)+len(m.reduceJobs)+len(m.runningReduceJobs) == 0 {
		ret = true
	}
	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.

	// Initialize variables
	inLen := len(files)
	m.mapJobs = make([]MrJob, 0, inLen)
	m.runningMapJobs = make([]*MrJob, 0, inLen)
	m.reduceJobs = make([]MrJob, 0, nReduce)
	m.runningReduceJobs = make([]*MrJob, 0, nReduce)
	m.runningMapJobsTimeMap = make(map[*MrJob]time.Time)
	m.runningReduceJobsTimeMap = make(map[*MrJob]time.Time)
	m.nReduce = nReduce
	m.hashKeyMap = make(map[string]bool)
	m.reduceGenerated = false

	// create mapjobs from input files
	for i, file := range files {
		m.mapJobs = append(m.mapJobs, MrJob{"map", RemoveDotTxt(file), file, i + 1, nReduce, ""})
	}

	m.server()
	return &m
}

// Helper function to create an "hold" MrJob
func createOnHoldJob() *MrJob {
	return &MrJob{"hold", "", "", 0, 0, ""}
}

// Helper function to create an "exit" MrJob
func createExitJob() *MrJob {
	return &MrJob{"exit", "", "", 0, 0, ""}
}

// Helper for create map job
func createMapJob(m *Master) *MrJob {
	newMapJob := &m.mapJobs[0]
	m.mapJobs = m.mapJobs[1:]
	m.runningMapJobs = append(m.runningMapJobs, newMapJob)

	m.runningMapJobsTimeMap[newMapJob] = time.Now()
	// generate uid and relate it with map job
	newMapJob.Hashkey = generateRandomString(16)
	m.hashKeyMap[newMapJob.Hashkey] = true

	return newMapJob
}

// Helper function to create an ReduceJob based on reduce ID
// "Generate all file names with all jobs and that reduceID"
func createReduceJob(m *Master) *MrJob {
	newReduceJob := &m.reduceJobs[0]
	m.reduceJobs = m.reduceJobs[1:]
	m.runningReduceJobs = append(m.runningReduceJobs, newReduceJob)

	m.runningReduceJobsTimeMap[newReduceJob] = time.Now()
	// generate uid and relate it with reduce job
	newReduceJob.Hashkey = generateRandomString(16)
	m.hashKeyMap[newReduceJob.Hashkey] = true

	return newReduceJob
}

// find and delete an element in a slice, return the resliced result
func deleteAndReslice(jobs []*MrJob, fileName string) []*MrJob {
	j := 0
	q := make([]*MrJob, len(jobs))
	for _, job := range jobs {
		if job.FileName != fileName {
			q[j] = job
			j++
		}
	}
	q = q[:j]
	return q
}

func (m *Master) checkPendingJobs() {
	// milliseconds
	timeout := int64(10000)

	newRunningMapJobs := []*MrJob{}
	// check running map jobs
	for _, mapJob := range m.runningMapJobs {
		curTime := time.Now()
		// if greater than certain period, we assume the worker is dead. Reassign the job.
		if curTime.Sub(m.runningMapJobsTimeMap[mapJob]).Nanoseconds()/1e6 > timeout {
			delete(m.runningMapJobsTimeMap, mapJob)
			m.mapJobs = append(m.mapJobs, *mapJob)
			delete(m.hashKeyMap, mapJob.Hashkey)
		} else {
			newRunningMapJobs = append(newRunningMapJobs, mapJob)
		}
	}
	m.runningMapJobs = newRunningMapJobs

	newRunningReduceJobs := []*MrJob{}
	// check running reduce jobs
	for _, reduceJob := range m.runningReduceJobs {
		curTime := time.Now()
		// if greater than certain period, we assume the worker is dead. Reassign the job.
		if curTime.Sub(m.runningReduceJobsTimeMap[reduceJob]).Nanoseconds()/1e6 > timeout {
			delete(m.runningReduceJobsTimeMap, reduceJob)
			m.reduceJobs = append(m.reduceJobs, *reduceJob)
			delete(m.hashKeyMap, reduceJob.Hashkey)
		} else {
			newRunningReduceJobs = append(newRunningReduceJobs, reduceJob)
		}
	}
	m.runningReduceJobs = newRunningReduceJobs
}

// generate a random string, input the length of string
func generateRandomString(length int) (str string) {
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

func generateReduceJobs(m *Master) {
	newReduceJobs := make([]MrJob, 0, m.nReduce)
	for nextReduceID := 1; nextReduceID <= m.nReduce; nextReduceID++ {
		fileNameBatch := ""
		for _, reduceJob := range m.reduceJobs {
			fileNameBatch = fileNameBatch + "inter" + "-" + reduceJob.FileName + "-" + strconv.Itoa(nextReduceID) + "|"
			// fileLocBatch := fileNameBatch
		}
		newReduceJob := MrJob{"reduce", fileNameBatch, fileNameBatch, nextReduceID, m.nReduce, ""}
		m.hashKeyMap[newReduceJob.Hashkey] = true
		// update Master's reduce tables

		newReduceJobs = append(newReduceJobs, newReduceJob)
	}
	m.reduceJobs = newReduceJobs
}
