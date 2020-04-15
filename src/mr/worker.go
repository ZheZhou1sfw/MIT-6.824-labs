package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	timeoutSeconds := 10 * time.Second

	// infinite loop here
	for {
		// try get a job from master
		req := RPCrequest{""}
		res := RPCresponse{}

		// also check time out
		c := make(chan error, 1)
		go func() { c <- call("Master.GetAJob", &req, &res) }()
		select {
		case err := <-c:
			if err != nil {
				log.Fatal("dialing in channel:", err)
			}
		case <-time.After(timeoutSeconds):
			// call timed out
			os.Exit(0) // exit with success
		}

		// call("Master.GetAJob", &req, &res)
		// depend on the job type (4 types), do different things

		jobType := res.CurJob.JobType
		switch jobType {
		case "hold":
			// wait for a while before request master again
			time.Sleep(200 * time.Millisecond)
		case "exit":
			os.Exit(0) // exit with success
		case "map":
			doMap(res.CurJob, mapf)
		case "reduce":
			doReduce(res.CurJob, reducef)
		}

	}

}

// doMap takes in an MrJob, output nReduce intermediate files for later use in reduce phase
// Return type: Error. Nil means success
func doMap(mapJob *MrJob, mapf func(string, string) []KeyValue) (err error) {
	// open the file and get all kv in kva
	content := readFileContent(mapJob.FileLoc)
	kva := mapf(mapJob.FileLoc, string(content))

	// assign kv according to ihash % nReduce to different intermediate files
	kvaArray := make([][]KeyValue, mapJob.NReduce)
	for _, kv := range kva {
		targetIdx := ihash(kv.Key) % mapJob.NReduce
		if kvaArray[targetIdx] == nil {
			// kvaArray[targetIdx] = make([]KeyValue, len(kva))
			kvaArray[targetIdx] = []KeyValue{}
		}
		kvaArray[targetIdx] = append(kvaArray[targetIdx], kv)
	}

	// create a tempFile(*os.File) -> realFileName(string) map that needs to be renamed if notify Succeeds
	tempFileMap := make(map[*os.File]string)

	// output these intermediate files
	for i := 0; i < mapJob.NReduce; i++ {
		interFileName := getIntermediate(mapJob.FileName, mapJob.ID, i+1)

		curKva := kvaArray[i]
		//fileHandle, err := openOrCreate(interFileName)
		tempFileHandle, err := ioutil.TempFile("", interFileName+"*|")
		if err != nil {
			log.Fatalf("failed to create tempFile in map phase: %v", interFileName)
		}
		tempFileMap[tempFileHandle] = interFileName

		// output all related kv pairs into the corresponding nReduce'th file
		enc := json.NewEncoder(tempFileHandle)
		for _, kv := range curKva {
			err = enc.Encode(&kv)
		}
		err = tempFileHandle.Close()
		if err != nil {
			log.Fatalf("cannot close the file in map phase '%v'", interFileName)
		}
	}

	// notify master about finish
	notifyRes := NotifyResponse{false}
	call("Master.NotifyFinish", mapJob, &notifyRes)

	// if acknowledged, rename the tempFile
	if notifyRes.Ack {
		for tempFileHandle, realFileName := range tempFileMap {
			err = os.Rename(tempFileHandle.Name(), realFileName)
			if err != nil {
				log.Fatalf("cannot rename tempfile %v to real fileName '%v'", tempFileHandle.Name(), realFileName)
			}
		}
	} else {
		fmt.Println("cannot receive ack in map phase")
	}

	return
}

// doMap takes in an MrJob, output nReduce intermediate files for later use in reduce phase
// Return type: Error. Nil means success
func doReduce(reduceJob *MrJob, reducef func(string, []string) string) (err error) {
	// the kva  array that stores all kva  key-values pairs
	kva := []KeyValue{}

	// read in all files and append kva to []kva
	files := strings.Split(reduceJob.FileLoc, "|")
	for _, fileLocByte := range files {
		fileLocString := string(fileLocByte)
		fileHandle, _ := openOrCreate(fileLocString)
		// if err != nil {
		// 	log.Fatalf("failed to create/open the file in reduce phase: %v", fileLocString)
		// }
		// read in using decoder
		dec := json.NewDecoder(fileHandle)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}

	// sort by key
	sort.Sort(ByKey(kva))

	// create the file to write the output to
	oname := getFinalFileName(reduceJob.ID)
	tempFileHandle, err := ioutil.TempFile("", oname+"*|")
	if err != nil {
		log.Fatalf("failed to create tempFile in reduce phase: %v", oname)
	}

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tempFileHandle, "%v %v\n", kva[i].Key, output)

		i = j
	}

	err = tempFileHandle.Close()
	if err != nil {
		log.Fatalf("cannot close the file in reduce phase '%v'", oname)
	}

	// notify master about finishing reduce
	notifyRes := NotifyResponse{false}
	call("Master.NotifyFinish", reduceJob, &notifyRes)

	// if acknowledged, rename the tempFile
	if notifyRes.Ack {
		err = os.Rename(tempFileHandle.Name(), oname)
		if err != nil {
			log.Fatalf("cannot rename tempfile %v to real fileName '%v'", tempFileHandle.Name(), oname)
		}
	}

	return
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) error {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	return err
}

// helper function for opening a file and return the content
// If success return
func readFileContent(fileLoc string) (content []byte) {
	file, err := os.Open(fileLoc)
	if err != nil {
		log.Fatalf("cannot open %v", fileLoc)
	}
	content, err = ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileLoc)
	}
	file.Close()
	return
}

// helper function that generate concatenated string as the name for intermediate result
func getIntermediate(fileName string, mapjobID int, nth int) string {
	// result := fileName + "-" + strconv.Itoa(mapjobID) + "-" + strconv.Itoa(nth)
	result := "inter" + "-" + fileName + "-" + strconv.Itoa(nth)
	return result
}

// helper function that generate the final output name
func getFinalFileName(nth int) string {
	result := "mr-out-" + strconv.Itoa(nth)
	return result
}

// create a file or open an existing file with given file name
func openOrCreate(fileLoc string) (target *os.File, err error) {
	if fileExists(fileLoc) {
		target, err = os.Open(fileLoc)
	} else {
		target, err = os.Create(fileLoc)
	}
	return
}

// fileExists checks if a file exists and is not a directory before we
// try using it to prevent further errors.
func fileExists(fileLoc string) bool {
	info, err := os.Stat(fileLoc)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}
