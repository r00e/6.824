package mr

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

type ByKey []KeyValue

func (a ByKey) Len() int {
	return len(a)
}
func (a ByKey) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}
func (a ByKey) Less(i, j int) bool {
	return a[i].Key < a[j].Key
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

//
// main/mrworker.go calls this function.
//
var uuid string

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	uuid = genUUID()
	// registerWorker(uuid) // 没用上
	for true {
		task := getTask()
		switch task.TaskType {
		case Map:
			doMapWork(&task, mapf)
		case Reduce:
			doReduceWork(&task, reducef)
		case AllTasksDone:
			// fmt.Println("All tasks are done...")
			break
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}

func registerWorker(uuid string) {
	args := RpcRegisterWorkerArgs{}
	args.Uuid = uuid

	reply := RpcRegisterWorkerReply{}
	response := call("Coordinator.RegisterWorkerHandler", &args, &reply)
	if !response {
		fmt.Println("Stop due to call error when call Coordinator.RegisterWorkerHandler...")
		os.Exit(0)
	}
}

func genUUID() string {
	// generate 32 bits timestamp
	unix32bits := uint32(time.Now().UTC().Unix())

	buff := make([]byte, 12)

	numRead, err := rand.Read(buff)

	if numRead != len(buff) || err != nil {
		panic(err)
	}
	return fmt.Sprintf("%x-%x-%x-%x-%x-%x", unix32bits, buff[0:2], buff[2:4], buff[4:6], buff[6:8], buff[8:])
}

func doReduceWork(task *TaskRpcReply, reducef func(string, []string) string) {
	intermediateKV := []KeyValue{}
	for _, fileName := range task.IntermediateFiles {
		file, err := os.Open(fileName)
		defer file.Close()
		if err != nil {
			log.Fatal("cannot open %v", file)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediateKV = append(intermediateKV, kv)
		}
	}
	sort.Sort(ByKey(intermediateKV))
	outputName := "mr-out-" + uuid + "-" + strconv.Itoa(task.ReduceIdx)
	// fmt.Printf("output file name is : %v\n", outputName)
	outputFile, _ := os.Create(outputName)
	i := 0
	for i < len(intermediateKV) {
		j := i + 1
		for j < len(intermediateKV) && intermediateKV[i].Key == intermediateKV[j].Key {
			j++
		}
		mergedValuesOfSameKeys := []string{}
		for k := i; k < j; k++ {
			mergedValuesOfSameKeys = append(mergedValuesOfSameKeys, intermediateKV[k].Value)
		}
		reduced := reducef(intermediateKV[i].Key, mergedValuesOfSameKeys)
		fmt.Fprintf(outputFile, "%v %v\n", intermediateKV[i].Key, reduced)
		i = j
	}
	notifyCoordinatorTaskDone(task.TaskType, strconv.Itoa(task.ReduceIdx))
	// fmt.Println("Reduce work is done...")
}

func doMapWork(task *TaskRpcReply, mapf func(string, string) []KeyValue) {
	piece, err := os.Open(task.PieceFileName)
	if err != nil {
		log.Fatal("Error when open file piece : %v", task.PieceFileName)
	}
	pieceContent, err := ioutil.ReadAll(piece)
	if err != nil {
		log.Fatal("Error when read file piece : %v", task.PieceFileName)
	}
	keyValuePairArr := mapf(task.PieceFileName, string(pieceContent))
	keyValuePairArrangedByKey := Partition(keyValuePairArr, task.NReduce, task)
	for i := 0; i < task.NReduce; i++ {
		intermediateFileName := WriteToJSONFile(keyValuePairArrangedByKey[i], task.MapTaskNum, i)
		syncIntermediateFileWithCoordinator(intermediateFileName, i)
	}
	notifyCoordinatorTaskDone(task.TaskType, task.PieceFileName)
	// fmt.Println("Map work is done...")
}

func notifyCoordinatorTaskDone(taskType TaskType, taskDoneInfo string) {
	args := RpcTaskDoneArgs{}
	args.TaskType = taskType
	args.TaskDoneInfo = taskDoneInfo

	reply := RpcTaskDoneReply{}
	response := call("Coordinator.TaskDoneHandler", &args, &reply)
	if !response {
		fmt.Println("Stop due to call error when call Coordinator.TaskDoneHandler...")
		os.Exit(0)
	}
}

func syncIntermediateFileWithCoordinator(intermediateFileName string, reduceIdx int) {
	args := IntermediateFileArgs{}
	args.IntermediateFileName = intermediateFileName
	args.ReduceIdx = reduceIdx

	reply := IntermediateFileReply{}

	if !call("Coordinator.IntermediateFileHandler", &args, &reply) {
		fmt.Println("Stop due to call error when call Coordinator.IntermediateFileHandler...")
		os.Exit(0)
	}
}

// Intermediate file is a JSON file
// Its name format is : mr-uuid-mapTaskNum-reduceIndex
func WriteToJSONFile(kvpairs []KeyValue, mapTaskNum int, reduceIdx int) string {
	jsonFileName := "mr-" + uuid + "-" + strconv.Itoa(mapTaskNum) + "-" + strconv.Itoa(reduceIdx)
	file, _ := os.Create(jsonFileName)

	encode := json.NewEncoder(file)
	for _, kv := range kvpairs {
		err := encode.Encode(&kv)
		if err != nil {
			log.Fatal("Error happend when encoding kv pair : %v", err)
		}
	}
	return jsonFileName
}

// var tmp int = 1

func Partition(keyValuePairArr []KeyValue, nReduce int, task *TaskRpcReply) [][]KeyValue {
	keyValuePair := make([][]KeyValue, nReduce)
	for _, kv := range keyValuePairArr {
		reduceIdx := ihash(kv.Key) % nReduce
		// if kv.Key == "ADVENTURES" {
		// 	fmt.Printf("ADVENTURES count is %v, this one is in %v, map num is %v, reduceIdx is %v\n", tmp, task.PieceFileName, task.MapTaskNum, reduceIdx)
		// 	tmp++
		// }
		keyValuePair[reduceIdx] = append(keyValuePair[reduceIdx], kv)
	}
	return keyValuePair
}

func getTask() TaskRpcReply {
	args := TaskRpcArgs{}
	reply := TaskRpcReply{}

	if !call("Coordinator.TaskHandler", &args, &reply) {
		fmt.Println("Stop due to call error when call Coordinator.TaskHandler...")
		os.Exit(0)
	}

	return reply
}

//
// example function to show how to make an RPC call to the coordinator.
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
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	// fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
