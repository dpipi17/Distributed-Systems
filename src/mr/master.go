package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "time"
import "strings"
import "sync"
import "strconv"

type Task struct {
	taskType int
	files []string 
	workingDirectory string
	isProcessing bool
	isDone bool
}

type Master struct {
	tasks []Task
	nReduce int
	mapTaskLeft int
	reduceTaskLeft int
	workingDirectory string
	lock sync.Mutex
}

func (m *Master) GetInitParameters(args *InitParametersRequest, reply *InitParametersResponse) error {
	reply.NReduce = m.nReduce
	return nil
} 

func (m *Master) GetTask(args *GetTaskRequest, reply *GetTaskResponse) error {
	m.lock.Lock()

	reply.TaskType = WAIT_TASK
	if m.mapTaskLeft > 0 || m.reduceTaskLeft > 0 {
		for index, task := range m.tasks {
			if task.isDone == false && task.isProcessing == false {
				reply.TaskType = task.taskType
				reply.FileNames = task.files
				reply.DirectoryName = task.workingDirectory
				reply.WorkerId = index

				m.tasks[index].isProcessing = true
				go m.checkTaskStatus(index, task.taskType)
				break
			}
		}
	}

	m.lock.Unlock()
	return nil
}

func (m *Master) checkTaskStatus(index, taskType int) {
	time.Sleep(10 * time.Second)

	m.lock.Lock()
	if index < len(m.tasks) {
		task := m.tasks[index]
		if (task.isDone == false && task.taskType == taskType) {
			m.tasks[index].isProcessing = false
		}
	}
	m.lock.Unlock()
} 

func (m *Master) CompleteTask(args *CompleteTaskRequest, reply *CompleteTaskResponse) error {
	m.lock.Lock()
	if args.Index < len(m.tasks) {
		task := m.tasks[args.Index]
		if (task.isDone == false && task.taskType == args.TaskType && task.isProcessing) {
			m.tasks[args.Index].isDone = true

			if task.taskType == MAP_TASK {
				m.mapTaskLeft -= 1
				if m.mapTaskLeft == 0 {
					m.lock.Unlock()
					go m.prepareForReduceTasks()
					return nil
				}
			} else if task.taskType == REDUCE_TASK {
				m.reduceTaskLeft -= 1
			}
		}
	}
	m.lock.Unlock()
	
	return nil
}

func (m *Master) prepareForReduceTasks() {
	m.lock.Lock()
	m.tasks = nil
	m.lock.Unlock()

	list := m.getAllFilesFromDirectory() 
	for reducerId := 0; reducerId < m.nReduce; reducerId++ {
		
		var fileNames []string
		for _, fileName := range list {
			if strings.HasSuffix(fileName, "-" + strconv.Itoa(reducerId)) {
				fileNames = append(fileNames, fileName)
			}
		}

		m.lock.Lock()
		m.tasks = append(m.tasks, Task{REDUCE_TASK, fileNames, m.workingDirectory, false, false})
		if m.reduceTaskLeft == -1 {
			m.reduceTaskLeft = m.nReduce
		}
		m.lock.Unlock()
	}
}

func (m *Master) getAllFilesFromDirectory() []string {
	file, err := os.Open(m.workingDirectory)
    if err != nil {
        log.Fatalf("failed opening directory: %s", err)
	}
	
	list,_ := file.Readdirnames(0)
	return list
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
	m.lock.Lock()
	done := m.mapTaskLeft == 0 && m.reduceTaskLeft == 0
	m.lock.Unlock()
	
	if done {
		list := m.getAllFilesFromDirectory()
		for _, fileName := range list {
			if strings.HasPrefix(fileName, INTERMEDIATE_FILE_PREFIX) {
				os.Remove(fileName)
			}
		}
	}
	
	return done
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	m.mapTaskLeft = len(files)
	m.reduceTaskLeft = -1
	m.nReduce = nReduce
	m.lock = sync.Mutex{}

	workingDirectory, err := os.Getwd()
	if err != nil {
		log.Fatalf("failed get current directory: %s", err)
	}
	m.workingDirectory = workingDirectory

	for _, filename := range files {
		m.tasks = append(m.tasks, Task {MAP_TASK, []string{filename}, m.workingDirectory, false, false})
	}

	m.server()
	return &m
}
