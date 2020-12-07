package kvraft

import (
	"../labgob"
	"../labrpc"
	"log"
	"../raft"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Method  Method
	Key     string
	Value   string
	Server  int
	MutId   int
	Clerk   int64
	Request int
}

type MutAndCond struct {
	mutex        *sync.Mutex
	cond         *sync.Cond
	broadcasted  *bool
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	dataBase 	map[string]string
	locksMap    map[int]MutAndCond	
	maxReqMap   map[int64]int
	getResults  map[int]string
	mutCounter  int

}

func (kv *KVServer) getMethodFromOp(op string) Method {
	if op == "Put" {
		return PUT
	}
	return APPEND 
}

func (kv *KVServer) getMutAndCond() (MutAndCond, int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	mutId := kv.mutCounter
	kv.mutCounter += 1

	m := &sync.Mutex{}
	c := sync.NewCond(m)
	broadcasted := false
	res := MutAndCond{mutex: m, cond: c, broadcasted: &broadcasted}
	kv.locksMap[mutId] = res
	
	return res, mutId
}

func (kv *KVServer) sleepThenBroadCast(mutAndCond MutAndCond) {
	time.Sleep(time.Duration(WAIT_TIME) * time.Millisecond)

	mutAndCond.mutex.Lock()
	*mutAndCond.broadcasted = true
	mutAndCond.cond.Broadcast()
	mutAndCond.mutex.Unlock()
}

func (kv *KVServer) waitOnCond(mutAndCond MutAndCond) {
	go kv.sleepThenBroadCast(mutAndCond)
	mutAndCond.mutex.Lock()
	if *mutAndCond.broadcasted == false {
		mutAndCond.cond.Wait()
	}
	mutAndCond.mutex.Unlock()
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	mutAndCond, mutId := kv.getMutAndCond()
	
	index, _, isLeader := kv.rf.Start(Op{Method: GET, Key: args.Key, Value: "", Server: kv.me, MutId: mutId})
	if isLeader == false {
		reply.Err = ErrWrongLeader
		return
	}

	kv.waitOnCond(mutAndCond)
	
	kv.mu.Lock()
	value, ok := kv.getResults[index]
	if ok {
		reply.Value = value
		reply.Err = OK
	} else {
		reply.Err = ErrNoKey
	}
	kv.mu.Unlock()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock() 
	if kv.maxReqMap[args.ClerkId] > args.ReqId {
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock() 

	mutAndCond, mutId := kv.getMutAndCond()
	command := Op{Method: kv.getMethodFromOp(args.Op), Key: args.Key, Value: args.Value, Server: kv.me, MutId: mutId, Clerk: args.ClerkId, Request: args.ReqId}
	_, _, isLeader := kv.rf.Start(command)
	if isLeader == false {
		reply.Err = ErrWrongLeader
		return
	}

	kv.waitOnCond(mutAndCond)
	
	kv.mu.Lock() 
	if kv.maxReqMap[args.ClerkId] >= args.ReqId {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock() 
	reply.Err = ErrNoKey
}

func (kv *KVServer) updateDataBase() {
	for {
		appliedMessage := <- kv.applyCh
		op := appliedMessage.Command.(Op)
		
		kv.mu.Lock()
		if op.Method != GET && op.Request > kv.maxReqMap[op.Clerk] {
			if op.Method == PUT {
				kv.dataBase[op.Key] = op.Value	
				kv.maxReqMap[op.Clerk] = op.Request
			} else if op.Method == APPEND {
				kv.dataBase[op.Key] = kv.dataBase[op.Key] + op.Value
				kv.maxReqMap[op.Clerk] = op.Request
			}
		}

		if op.Server == kv.me {			
			if op.Method == GET {
				kv.getResults[appliedMessage.CommandIndex] = kv.dataBase[op.Key]
			}

			mutAndCond, ok := kv.locksMap[op.MutId]
			if ok {
				mutAndCond.mutex.Lock()
				*mutAndCond.broadcasted = true
				mutAndCond.cond.Broadcast()
				mutAndCond.mutex.Unlock()
			}
		}

		kv.mu.Unlock()
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.dataBase = make(map[string]string)
	kv.locksMap = make(map[int]MutAndCond)
	kv.maxReqMap = make(map[int64]int)
	kv.getResults = make(map[int]string)
	kv.mutCounter = 1

	// You may need initialization code here.
	go kv.updateDataBase()

	return kv
}
