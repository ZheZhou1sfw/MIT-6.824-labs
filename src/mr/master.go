package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Master struct {
	// Your definitions here.
	mapJobs           []MrJob
	reduceJobs        []MrJob
	runningMapJobs    []MrJob
	runningReduceJobs []MrJob
	reduceJobID       []int
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) getAJob(req RPCrequest, res *RPCresponse) (err error) {
	var toDeliver MrJob
	if m.Done() {
		// err = errors.New("done")
		toDeliver = MrJob{"exitPlease", "", "", 0, 0}
	} else if len(m.mapJobs)+len(m.runningMapJobs) > 0 {
		// deliver a map job if there is any map job left
		if len(m.mapJobs) > 0 {
			toDeliver = m.mapJobs[0]
			m.mapJobs = m.mapJobs[1:]
			m.runningMapJobs = append(m.runningMapJobs, toDeliver)

			// there are some map job processing now. Let the worker wait
		} else {
			toDeliver = createOnHoldJob()
		}
		// !!!!!!!!!!!!!!!!!! below needs to be changed to fit each workerID
	} else {
		toDeliver = m.reduceJobs[0]
		m.reduceJobs = m.reduceJobs[1:]
	}
	res.curJob = toDeliver
	return // err is nil by default
}

// notify master one a worker finished a job
func (m *Master) notifyFinish(job MrJob, res *ExampleReply) (err error) {
	// map job finished
	if job.jobType == "map" {
		m.runningMapJobs = deleteAndReslice(m.runningMapJobs, job.fileName)
		newReduceJob := job // make a copy
		m.reduceJobs = append(m.reduceJobs, job)
	}
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
	if len(m.mapJobs)+len(m.reduceJobs)+len(m.runningMapJobs)+len(m.runningReduceJobs) == 0 {
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
	m.runningMapJobs = make([]MrJob, 0, inLen)
	m.reduceJobs = make([]MrJob, 0, nReduce)
	m.runningReduceJobs = make([]MrJob, 0, nReduce)

	// create mapjobs from input files
	for i, file := range files {
		m.mapJobs = append(m.mapJobs, MrJob{"map", removeDotTxt(file), file, i + 1, nReduce})
	}

	m.server()
	return &m
}

// Helper function to create an "hold" MrJob
func createOnHoldJob() MrJob {
	return MrJob{"hold", "", "", 0, 0}
}

// find and delete an element in a slice, return the resliced result
func deleteAndReslice(jobs []MrJob, fileName string) []MrJob {
	j := 0
	q := make([]MrJob, len(jobs))
	for _, job := range jobs {
		if job.fileName == fileName {
			q[j] = job
			j++
		}
	}
	q = q[:j]
	return q
}
