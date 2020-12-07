package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "time"
import "io/ioutil"
import "encoding/json"
import "sort"

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type WorkerObj struct {
	workerId int
	nReduce int
}

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func (w *WorkerObj) completeMapTask(mapf func(string, string) []KeyValue, fileNames []string, workingDir string) {
	intermediate := []KeyValue{}
	for _, filename := range fileNames {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))
		intermediate = append(intermediate, kva...)
	}

	tempFilesNames := w.createTempFiles(workingDir)

	kvMap := make(map [string][]KeyValue)
	for _, kv := range intermediate {
		elem, ok := kvMap[kv.Key]
		if ok {
			kvMap[kv.Key] = append(elem, kv)
		} else {
			kvMap[kv.Key] = []KeyValue{kv}
		}
	}

	for key, kvs := range kvMap {
		reducerId := ihash(key) % w.nReduce
		tempFile, err := os.OpenFile(tempFilesNames[reducerId], os.O_APPEND|os.O_RDWR, os.ModePerm)
		if err != nil{
			log.Fatalf("cannot open file %v error: %v", tempFilesNames[reducerId], err)
		}
		enc := json.NewEncoder(tempFile)
		for _, kv := range kvs {
			err := enc.Encode(kv)
			if err != nil{
				log.Fatalf("error while encoding: %v", err)
			}
		}
		tempFile.Close()
	}

	w.renameTempFiles(tempFilesNames, workingDir)
	w.callCompleteTask(MAP_TASK)
}

func (w *WorkerObj) createTempFiles(workingDir string) []string {
	var tempFilesNames []string
	
	for reducerId := 0; reducerId < w.nReduce; reducerId++ {
		tempFileName := fmt.Sprintf(INTERMEDIATE_FILE_PREFIX + "-temp-%v-%v", w.workerId, reducerId)

		tempFile, err := ioutil.TempFile(workingDir, tempFileName)
		if err != nil {
			log.Fatalf("cannot create temp file %v", tempFileName)
		}
		tempFile.Close()
		tempFilesNames = append(tempFilesNames, tempFile.Name())
	}

	return tempFilesNames
}

func (w *WorkerObj) renameTempFiles(tempFileNames []string, workingDir string) {
	for reducerId, tempFileName := range tempFileNames {
		realFilename := workingDir + "/" + fmt.Sprintf(INTERMEDIATE_FILE_PREFIX + "-%v-%v", w.workerId, reducerId)
		defer os.Remove(tempFileName)
		os.Rename(tempFileName, realFilename)
	}
} 

func (w *WorkerObj) completeReduceTask(reducef func(string, []string) string, fileNames []string, workingDir string) {
	intermediate := w.getIntermediatekvListFromFiles(fileNames)
	sort.Sort(ByKey(intermediate))

	tempFileName := fmt.Sprintf(INTERMEDIATE_FILE_PREFIX + "-out-%v", w.workerId)
	tempFile, err := ioutil.TempFile(workingDir, tempFileName)
	if err != nil {
		log.Fatalf("cannot create temp file %v", tempFileName)
	}
	defer os.Remove(tempFile.Name())

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	tempFile.Close()

	realFilename := workingDir + "/" + fmt.Sprintf("mr-out-%v", w.workerId)
	os.Rename(tempFile.Name(), realFilename)

	w.callCompleteTask(REDUCE_TASK)
}

func (w *WorkerObj) getIntermediatekvListFromFiles(fileNames []string) []KeyValue {
	var kvList []KeyValue

	for _, fileName := range fileNames {
		file, err := os.OpenFile(fileName, os.O_APPEND|os.O_RDWR, os.ModePerm)
		if err != nil {
			log.Fatalf("cannot open file %v error: %v", fileName, err)
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kvList = append(kvList, kv)
		}
	}

	return kvList
}

func (w *WorkerObj) callCompleteTask(taskType int) {
	args := CompleteTaskRequest {taskType, w.workerId}
	response := CompleteTaskResponse {}
	call("Master.CompleteTask", &args, &response)
}

func (w *WorkerObj) initParametersCall() {
	args := InitParametersRequest {}
	response := InitParametersResponse {}
	
	if call("Master.GetInitParameters", &args, &response) == false {
		fmt.Println("Master not responding")
		os.Exit(-1)
	}

	w.nReduce = response.NReduce
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	worker := WorkerObj{}
	worker.initParametersCall()
	
	for {
		args := GetTaskRequest {}
		response := GetTaskResponse {}
		
		if call("Master.GetTask", &args, &response) == false {
			fmt.Println("Master not responding")
			os.Exit(-1)
		}

		worker.workerId = response.WorkerId
		switch response.TaskType {
			case MAP_TASK:
				worker.completeMapTask(mapf, response.FileNames, response.DirectoryName)
			case REDUCE_TASK:
				worker.completeReduceTask(reducef, response.FileNames, response.DirectoryName)
			case WAIT_TASK:
				time.Sleep(time.Second)
		}
	}

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
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
