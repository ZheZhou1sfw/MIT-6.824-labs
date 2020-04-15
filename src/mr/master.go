package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

type Master struct {
	// Your definitions here.
	mapJobs                    []MrJob
	reduceJobs                 []MrJob
	runningMapJobs             []*MrJob
	runningMapJobsTimeMap      map[*MrJob]time.Time
	runningReduceWorker        []int
	runningReduceWorkerTimeMap map[int]time.Time
	finishedReduceWorker       []int
	nReduce                    int
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) GetAJob(req RPCrequest, res *RPCresponse) (err error) {
	// -------Debug Info -------- //
	// fmt.Println("want a job")
	// fmt.Printf("Number of runningReduceWorker: %v || Number of finishedReducerWorker: %v", len(m.runningMapJobs), len(m.finishedReduceWorker))
	// fmt.Println("")
	// fmt.Println(m.runningReduceWorker)
	// fmt.Println(m.runningReduceWorkerTimeMap)
	// fmt.Println(m.finishedReduceWorker)
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
			toDeliver = createMapJob(m)
		} else {
			toDeliver = createOnHoldJob()
		}
	} else {
		nextReduceID := findNextReduceWorkerID(m)
		// can't successfully create a reduce job
		if nextReduceID == -1 {
			// all jobs done
			if len(m.runningReduceWorker) == 0 {
				toDeliver = createExitJob()
				// some reduce jobs are processing but no more undispatched jobs
			} else {
				toDeliver = createOnHoldJob()
			}
			// could successfully dispatch a reduce job
		} else {
			// The reduce job should base on all map job, but is for one specific reduceID (nReduceIDs in total)
			toDeliver = createReduceJob(m, nextReduceID)
		}
	}
	res.CurJob = toDeliver
	return // err is nil by default
}

// notify master one a worker finished a job
func (m *Master) NotifyFinish(job *MrJob, res *NotifyResponse) (err error) {
	if job.JobType == "map" {
		m.runningMapJobs = deleteAndReslice(m.runningMapJobs, job.FileName)
		newReduceJob := *job // make a copy
		newReduceJob.JobType = "reduce"
		m.reduceJobs = append(m.reduceJobs, newReduceJob)
	} else if job.JobType == "reduce" {
		// find and remove runningReduceJobID
		reduceID := job.ID
		for i, rid := range m.runningReduceWorker {
			if rid == reduceID {
				m.runningReduceWorker = append(m.runningReduceWorker[:i], m.runningReduceWorker[i+1:]...)
				m.finishedReduceWorker = append(m.finishedReduceWorker, reduceID)
				delete(m.runningReduceWorkerTimeMap, rid)
				break
			}
		}
	} else {
		log.Fatal("Cannot notifyFinish this job type" + job.JobType)
	}

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
	if len(m.mapJobs)+len(m.runningMapJobs)+len(m.runningReduceWorker) == 0 && len(m.finishedReduceWorker) == m.nReduce {
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
	m.runningReduceWorker = make([]int, 0, nReduce)
	m.runningMapJobsTimeMap = make(map[*MrJob]time.Time)
	m.finishedReduceWorker = make([]int, 0, nReduce)
	m.runningReduceWorkerTimeMap = make(map[int]time.Time)
	m.nReduce = nReduce

	// create mapjobs from input files
	for i, file := range files {
		m.mapJobs = append(m.mapJobs, MrJob{"map", RemoveDotTxt(file), file, i + 1, nReduce})
	}

	m.server()
	return &m
}

// Helper function to create an "hold" MrJob
func createOnHoldJob() *MrJob {
	return &MrJob{"hold", "", "", 0, 0}
}

// Helper function to create an "exit" MrJob
func createExitJob() *MrJob {
	return &MrJob{"exit", "", "", 0, 0}
}

// Helper for create map job
func createMapJob(m *Master) *MrJob {
	newMapJob := &m.mapJobs[0]
	m.mapJobs = m.mapJobs[1:]
	m.runningMapJobs = append(m.runningMapJobs, newMapJob)

	m.runningMapJobsTimeMap[newMapJob] = time.Now()

	return newMapJob
}

// Helper function to create an ReduceJob based on reduce ID
// "Generate all file names with all jobs and that reduceID"
func createReduceJob(m *Master, nextReduceID int) *MrJob {
	fileNameBatch := ""
	// fileNameBatch, fileLocBatch := "", ""
	for _, reduceJob := range m.reduceJobs {
		fileNameBatch = fileNameBatch + "inter" + "-" + reduceJob.FileName + "-" + strconv.Itoa(nextReduceID) + "|"
		// fileLocBatch := fileNameBatch
	}
	newReduceJob := MrJob{"reduce", fileNameBatch, fileNameBatch, nextReduceID, m.nReduce}

	// update Master's reduce tables
	m.runningReduceWorker = append(m.runningReduceWorker, nextReduceID)

	m.runningReduceWorkerTimeMap[nextReduceID] = time.Now()

	return &newReduceJob
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

// Helper function that returns the next unassigned reduceWorkerID (nReduce in total)
// -1 means all used
func findNextReduceWorkerID(m *Master) int {
	arr := make([]int, 0, m.nReduce)
	for _, id := range append(m.finishedReduceWorker, m.runningReduceWorker...) {
		arr = append(arr, id)
	}
	// arr is empty, return first
	if len(arr) == 0 {
		return 1
	}

	sort.Ints(arr)

	id := 1
	for idx := 0; idx < len(arr); idx, id = idx+1, id+1 {
		// missing id here
		if arr[idx] != id {
			return id
		}
	}
	if id <= m.nReduce {
		return id
	}
	return -1
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
		} else {
			newRunningMapJobs = append(newRunningMapJobs, mapJob)
		}
	}
	m.runningMapJobs = newRunningMapJobs

	newRunningReduceWorker := []int{}
	// check running reduce jobs
	for _, reduceWorkerID := range m.runningReduceWorker {
		curTime := time.Now()
		// if greater than certain period, we assume the worker is dead. Reassign the job.
		if curTime.Sub(m.runningReduceWorkerTimeMap[reduceWorkerID]).Nanoseconds()/1e6 > timeout {
			delete(m.runningReduceWorkerTimeMap, reduceWorkerID)
		} else {
			newRunningReduceWorker = append(newRunningReduceWorker, reduceWorkerID)
		}
	}
	m.runningReduceWorker = newRunningReduceWorker
}
