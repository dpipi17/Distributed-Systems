package kvraft

import "../labrpc"
import "crypto/rand"
import "math/big"
import "sync"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	id		 int64
	leaderId int 
	reqId    int 
	mu       sync.Mutex
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.id = nrand()
	ck.leaderId = 0 
	ck.reqId = 0
	return ck
}

func (ck *Clerk) getNextReqId() int {
	ck.mu.Lock()
	ck.reqId += 1
	res := ck.reqId
	ck.mu.Unlock()
	return res
}

func (ck *Clerk) getLeader() int {
	ck.mu.Lock()
	res := ck.leaderId
	ck.mu.Unlock()
	return res
}

func (ck *Clerk) updateLeader(newLeader int) {
	ck.mu.Lock()
	ck.leaderId = newLeader
	ck.mu.Unlock()
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	setverNum := len(ck.servers)

	server := ck.getLeader()
	for server < setverNum {
		args := GetArgs{Key: key}
		reply := GetReply{Err: "", Value: ""}
		ok := ck.servers[server].Call("KVServer.Get", &args, &reply)

		if ok && reply.Err == OK {
			ck.updateLeader(server)
			return reply.Value
		}

		if reply.Err == ErrWrongLeader || !ok {
			server = (server + 1) % setverNum
		}
	}
	
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.

	setverNum := len(ck.servers)
	reqId := ck.getNextReqId()

	server := ck.getLeader()
	for server < setverNum {
		args := PutAppendArgs{Key: key, Value: value, Op: op, ClerkId: ck.id, ReqId: reqId}
		reply := PutAppendReply{Err: ""}
		ok := ck.servers[server].Call("KVServer.PutAppend", &args, &reply)

		if ok && reply.Err == OK {
			ck.updateLeader(server)
			return
		}

		if reply.Err == ErrWrongLeader || !ok {
			server = (server + 1) % setverNum
		}
	}
	
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
