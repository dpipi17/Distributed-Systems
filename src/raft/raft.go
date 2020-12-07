package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import "sync/atomic"
import "../labrpc"
import "time"
import "math/rand"
import "sort"

import "bytes"
import "../labgob"

const (
	FOLLOWER = 0
	CANDIDATE = 1
	LEADER  = 2

	ELECTION_TIMEOUT_FROM = 300
	ELECTION_TIMEOUT_TO = 900
)


//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int 
	votedFor map[int]int
	logs []Log

	commitIndex int
	lastApplied int 

	nextIndex []int 
	matchIndex []int

	leaderId int
	lastLeaderHeartBeatTime time.Time
	lastVoteTime time.Time

	currStateStatus int
	applyCh chan ApplyMsg
}

type Log struct {
	Command interface{}
	Term int  
	Index int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).

	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.currStateStatus == LEADER
	rf.mu.Unlock()

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor map[int]int
	var logs []Log

	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&logs) != nil {
	  DPrintf("error");
	} else {
	  rf.currentTerm = currentTerm
	  rf.votedFor = votedFor
	  rf.logs = logs
	//   DPrintf("read on server: %v, term: %v, logs: %v", rf.me, rf.currentTerm, rf.logs)
	}
}




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateId int 
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	
	reply.Term = rf.currentTerm
	_, alreadyVotedOnThisTerm := rf.votedFor[args.Term]

	isAhead := false
	isUpToDate := true
	if len(rf.logs) > 0 {
		var lastLog Log
		lastLog = rf.logs[len(rf.logs) - 1]
		isAhead = (args.LastLogTerm > lastLog.Term) || (args.LastLogTerm == lastLog.Term && args.LastLogIndex > lastLog.Index)
		isUpToDate = (args.LastLogTerm > lastLog.Term) || (args.LastLogTerm == lastLog.Term && args.LastLogIndex >= lastLog.Index)
	}
	
	if args.Term < rf.currentTerm || (alreadyVotedOnThisTerm && isAhead == false) || isUpToDate == false  {
		reply.VoteGranted = false
		// DPrintf("server: %v not voted to: %v on candidate term: %v, on follower term: %v -- %v, %v", rf.me, args.CandidateId, args.Term, rf.currentTerm, alreadyVotedOnThisTerm, isUpToDate)
	} else {
		// DPrintf("server: %v granted vote to: %v on term: %v, follower term: %v", rf.me, args.CandidateId, args.Term, rf.currentTerm)
		reply.VoteGranted = true
		rf.currStateStatus = FOLLOWER
		rf.votedFor[args.Term] = args.CandidateId
		rf.currentTerm = args.Term
		rf.lastVoteTime = time.Now()
		rf.persist()
	}
	rf.mu.Unlock()
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}


//
// example AppendEntries RPC arguments structure.
//
type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries []Log
	LeaderCommit int
}

//
// example AppendEntries RPC reply structure.
//
type AppendEntriesReply struct {
	Term int
	Success bool
}

//
// example AppendEntries RPC handler.
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	reply.Term = rf.currentTerm
	if (args.Term >= rf.currentTerm) {
		rf.lastLeaderHeartBeatTime = time.Now()
		rf.currStateStatus = FOLLOWER
		rf.currentTerm = args.Term

		if args.PrevLogIndex == -1 {
			reply.Success = true
			rf.logs = args.Entries
		} else if args.PrevLogIndex < len(rf.logs) {
			if rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
				reply.Success = false
				rf.logs = rf.logs[:args.PrevLogIndex]
			} else {
				reply.Success = true
				for i := 0; i < len(args.Entries); i++ {
					index := i + args.PrevLogIndex + 1

					if index < len(rf.logs) {
						if rf.logs[index].Term != args.Entries[i].Term {
							rf.logs = rf.logs[:index]
							rf.logs = append(rf.logs, args.Entries[i])
						}
					} else {
						rf.logs = append(rf.logs, args.Entries[i])
					}
				}
			}
		}
	}

	if reply.Success && args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = args.LeaderCommit
		if args.LeaderCommit > len(rf.logs) - 1 {
			rf.commitIndex = len(rf.logs) - 1
		}	
		go rf.applyToStateMachine()
	}
	rf.persist()
	rf.mu.Unlock()
}

//
// example code to send a AppendEntries RPC to a server.
//
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	index = rf.nextIndex[rf.me]
	term = rf.currentTerm
	isLeader = rf.currStateStatus == LEADER

	if isLeader {
		rf.logs = append(rf.logs, Log{command, term, index})
		rf.persist()
		rf.nextIndex[rf.me] += 1
		rf.matchIndex[rf.me] = rf.nextIndex[rf.me] - 1

		rf.replicateLogEntriesOnEveryServer()
	}
	rf.mu.Unlock()


	return index + 1, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}


func (rf *Raft) checkPeriodicLeaderElection() {
	for {
		if rf.killed() {
			break
		}

		rf.mu.Lock()
		currStateStatus := rf.currStateStatus
		// DPrintf("server: %v, status: %v, term: %v, logs: %v", rf.me, rf.currStateStatus, rf.currentTerm, rf.logs)
		rf.mu.Unlock()

		if currStateStatus == FOLLOWER {
			rf.handleFollower()
		} else if currStateStatus == CANDIDATE {
			rf.handleCandidate()
		} else if currStateStatus == LEADER {
			rf.handleLeader()
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func (rf *Raft) handleFollower() {
	beginOfTimeOut := time.Now()
	electionTimeout := rand.Intn(ELECTION_TIMEOUT_TO - ELECTION_TIMEOUT_FROM) + ELECTION_TIMEOUT_FROM
	time.Sleep(time.Duration(electionTimeout) * time.Millisecond)
	
	rf.mu.Lock()
	if beginOfTimeOut.After(rf.lastLeaderHeartBeatTime) && beginOfTimeOut.After(rf.lastVoteTime) {
		rf.currStateStatus = CANDIDATE
	}
	rf.mu.Unlock()
}

func (rf *Raft) handleCandidate() {
	rf.mu.Lock()
	rf.currentTerm += 1
	rf.votedFor[rf.currentTerm] = rf.me
	rf.persist()

	LastLogIndex := -1
	LastLogTerm := -1
	if (len(rf.logs) > 0) {
		LastLogIndex = len(rf.logs) - 1
		LastLogTerm = rf.logs[LastLogIndex].Term	
	}
	args := RequestVoteArgs {Term : rf.currentTerm, CandidateId : rf.me, LastLogIndex: LastLogIndex, LastLogTerm: LastLogTerm}
	rf.mu.Unlock()
	
	votes := 1
	for server, _ := range rf.peers {
		if server != rf.me {
			
			go func(serverId int) {
				rf.mu.Lock()
				if (rf.currStateStatus != CANDIDATE) {
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()

				reply := RequestVoteReply {}
				rf.sendRequestVote(serverId, &args, &reply)
				
				rf.mu.Lock()
				if reply.VoteGranted {
					votes += 1
				}

				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.currStateStatus = FOLLOWER
					rf.persist()
				}
				rf.mu.Unlock()
			} (server)
		}
	}
	time.Sleep(50 * time.Millisecond)
	

	rf.mu.Lock()
	if votes > len(rf.peers) / 2 && rf.currStateStatus == CANDIDATE {
		rf.currStateStatus = LEADER
	}
	updatedStatus := rf.currStateStatus
	rf.mu.Unlock()
	
	if updatedStatus == CANDIDATE {
		electionTimeout := rand.Intn(ELECTION_TIMEOUT_TO - ELECTION_TIMEOUT_FROM) + ELECTION_TIMEOUT_FROM
		time.Sleep(time.Duration(electionTimeout) * time.Millisecond)
	} else if updatedStatus == LEADER {
		rf.reinitializeNextIndexAndMatchIndexArrays()
	}
}

func (rf *Raft) handleLeader() {
	rf.replicateLogEntriesOnEveryServer()
	time.Sleep(50 * time.Millisecond)

	rf.mu.Lock() 
	if rf.currStateStatus == LEADER {
		matchedIndexCopy := make([]int, len(rf.matchIndex))
		copy(matchedIndexCopy, rf.matchIndex)
		sort.Ints(matchedIndexCopy)
		indexToCommit := matchedIndexCopy[len(matchedIndexCopy) / 2]
		
		if indexToCommit >= 0 && rf.logs[indexToCommit].Term == rf.currentTerm {
			rf.commitIndex = indexToCommit
			go rf.applyToStateMachine()
		}
	}
	rf.mu.Unlock()
}

func (rf *Raft) replicateLogEntriesOnEveryServer() {
	for server, _ := range rf.peers {
		if server != rf.me {
			go func(serverId int) {
				rf.replicateLogEntries(serverId)
			} (server)
		}
	}
}

func (rf *Raft) applyToStateMachine() {
	rf.mu.Lock()
	start := rf.lastApplied + 1
	for i := start; i <= rf.commitIndex; i++ {
		rf.applyCh <- ApplyMsg {CommandValid: true, Command: rf.logs[i].Command, CommandIndex: rf.logs[i].Index + 1}
		rf.lastApplied += 1
	}
	rf.mu.Unlock()
}

func (rf *Raft) reinitializeNextIndexAndMatchIndexArrays() {
	rf.mu.Lock()
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = len(rf.logs)
		rf.matchIndex[i] = -1
	} 
	rf.mu.Unlock()
}

func (rf *Raft) replicateLogEntries(server int) bool {
	
	decreaseValue := 1
	for {
		if rf.killed() {
			break
		}

		rf.mu.Lock()
		prevLogIndex := rf.nextIndex[server] - 1
		if prevLogIndex < 0 {
			prevLogIndex = -1
		} else if prevLogIndex >= len(rf.logs) {
			prevLogIndex = len(rf.logs) - 1
		}
		prevLogTerm := -1
		entries := rf.logs

		if len(rf.logs) > 0 && prevLogIndex >= 0 {
			prevLogTerm = rf.logs[prevLogIndex].Term
			entries = rf.logs[prevLogIndex + 1:]
		}
		reply := AppendEntriesReply {}
		args := AppendEntriesArgs {	Term : rf.currentTerm, 
									LeaderId : rf.me, 
									PrevLogIndex: prevLogIndex, 
									PrevLogTerm: prevLogTerm,
									Entries: entries,
									LeaderCommit: rf.commitIndex}
		
		if (rf.currStateStatus != LEADER) {
			rf.mu.Unlock()
			return false
		}
		rf.mu.Unlock()
	
		rf.sendAppendEntries(server, &args, &reply)
		
		rf.mu.Lock()
		if args.Term != rf.currentTerm {
			rf.mu.Unlock()
			return false
		}

		if reply.Term > rf.currentTerm {
			rf.currStateStatus = FOLLOWER
			rf.currentTerm = reply.Term
			rf.persist()
			rf.mu.Unlock()
			return false
		}

		if (reply.Success) {
			rf.matchIndex[server] = prevLogIndex + len(entries)
			rf.nextIndex[server] = rf.matchIndex[server] + 1
			rf.mu.Unlock()
			return true
		}

		rf.nextIndex[server] -= decreaseValue
		rf.mu.Unlock()
		
		decreaseValue *= 2
	}
	return false
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = make(map[int]int)

	rf.commitIndex = -1
	rf.lastApplied = -1

	rf.nextIndex = make([]int, len(peers))
	for i := 0; i < len(rf.peers); i++ {
        rf.nextIndex[i] = 0
	} 
	
	rf.matchIndex = make([]int, len(peers))
	for i := 0; i < len(rf.peers); i++ {
        rf.matchIndex[i] = -1
	} 
	rf.currStateStatus = FOLLOWER
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.checkPeriodicLeaderElection()

	return rf
}
