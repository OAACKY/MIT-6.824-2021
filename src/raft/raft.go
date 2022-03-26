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

import (
	"6.824/labgob"
	"bytes"
	"sort"

	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
	crand "crypto/rand"
	"math/big"
)

type StateType int

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// define every raft server state
const (
	Follower StateType = iota
	Candidate
	Leader
)

// define the hold information about each log entry
type Entry struct {
	Cmd   interface{}
	Term  int
	Index int
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
	state       StateType
	currentTerm int
	votedFor    int
	log         []Entry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	isReceive   bool
	newCommand  bool
	isReady     *sync.Cond
	leaderReady bool
	applyCh     chan ApplyMsg
	applyCond   *sync.Cond
	startTicker bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = (rf.state == Leader)
	rf.mu.Unlock()
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
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
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var tempCurrentTerm int
	var tempVotedFor int
	var tempLog []Entry
	if d.Decode(&tempCurrentTerm) != nil || d.Decode(&tempVotedFor) != nil || d.Decode(&tempLog) != nil {
		DPrintf("decode error!")
	} else {
		rf.mu.Lock()
		rf.currentTerm = tempCurrentTerm
		rf.votedFor = tempVotedFor
		rf.log = tempLog
		rf.mu.Unlock()
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//set the election timeout
	//rf.isReceive = true
	//DPrintf("begin requestvote from [%d]-term%d to [%d]-term%d",args.CandidateId,args.Term,rf.me,rf.currentTerm)
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		return
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		if rf.state == Leader {
			if !rf.startTicker {
				rf.startTicker = true
				go rf.ticker()
			}
		}
		rf.state = Follower
	}
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		if len(rf.log) == 0 || rf.log[len(rf.log)-1].Term < args.LastLogTerm ||
			(rf.log[len(rf.log)-1].Term == args.LastLogTerm && rf.log[len(rf.log)-1].Index <= args.LastLogIndex) {
			if len(rf.log) != 0 {
				DPrintf("[%d]-last log term:%d vote for [%d]-last log term:%d", rf.me, rf.log[len(rf.log)-1].Term, args.CandidateId, args.LastLogTerm)
			} else {
				DPrintf("[%d]-last log term empty:%d vote for [%d]-last log term:%d", rf.me, rf.currentTerm, args.CandidateId, args.LastLogTerm)
			}
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
			rf.isReceive = true
		}
	}
	rf.persist()
	return
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

// AppendEntries RPC arguments structure
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

// AppendEntries RPC reply structure
type AppendEntriesReply struct {
	Term       int
	Success    bool
	MatchIndex int
	ConfIndex  int
	ConfTerm   int
}

// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	DPrintf("[%d] begin receive append entry from [%d] time:%v", rf.me, args.LeaderId, time.Now())
	defer rf.mu.Unlock()
	reply.Success = false
	reply.Term = rf.currentTerm
	reply.ConfIndex = -1
	reply.ConfTerm = -1
	//DPrintf("[%d]-term%d receive appendEntries from [%d]-term%d",rf.me,rf.currentTerm,args.LeaderId,args.Term)
	if args.Term < rf.currentTerm {
		return
	}
	rf.isReceive = true
	rf.votedFor = args.LeaderId
	if rf.state == Leader {
		rf.votedFor = args.LeaderId
		if !rf.startTicker {
			rf.startTicker = true
			go rf.ticker()
		}
	}
	rf.state = Follower
	rf.currentTerm = args.Term

	//if len(args.Entries) == 0 {
	//	// if heartbreak return
	//	reply.Success = true
	//	// check if should commit
	//	if args.LeaderCommit > rf.commitIndex {
	//		rf.commitIndex = args.LeaderCommit<rf.log[len(rf.log)-1].Index
	//	}
	//	return
	//}
	// don't have this entry
	DPrintf("[%d] receive append entry from [%d] len:%d time:%v", rf.me, args.LeaderId, len(rf.log), time.Now())
	if len(rf.log) < args.PrevLogIndex {
		reply.ConfIndex = len(rf.log)
		rf.persist()
		return
	}
	// empty or match the index and term
	if len(rf.log) == 0 || args.PrevLogIndex == 0 || rf.log[args.PrevLogIndex-1].Term == args.PrevLogTerm {
		// delete the conflict entry
		if len(rf.log) > args.PrevLogIndex {
			rf.log = rf.log[:args.PrevLogIndex]
		}
		if len(args.Entries) != 0 {
			rf.log = append(rf.log, args.Entries...)
		}
		reply.Success = true
		reply.MatchIndex = len(rf.log)
		DPrintf("[%d] accept append entry from [%d] len:%d", rf.me, args.LeaderId, len(rf.log))
		// update commitIndex
		if len(rf.log) > 0 && args.LeaderCommit > rf.commitIndex {
			if args.LeaderCommit < rf.log[len(rf.log)-1].Index {
				rf.commitIndex = args.LeaderCommit
			} else {
				rf.commitIndex = rf.log[len(rf.log)-1].Index
			}
		}
		if rf.commitIndex > rf.lastApplied {
			rf.applyCond.Broadcast()
			//go rf.applyNewMsg()
		}
	} else {
		// update conflict index
		reply.ConfTerm = rf.log[args.PrevLogIndex-1].Term
		for i := 0; i < len(rf.log); i++ {
			if rf.log[i].Term == reply.ConfTerm {
				reply.ConfIndex = i + 1
				break
			}
		}
	}
	rf.persist()
	return

}

// example for send a AppendEntries RPC to a server
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
	isLeader := false

	// Your code here (2B).
	if rf.killed() {
		return index, term, isLeader
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == Leader {
		isLeader = true

		for rf.leaderReady == false {
			DPrintf("what?")
			rf.isReady.Wait()
		}

		newEntry := Entry{}
		newEntry.Cmd = command
		newEntry.Term = rf.currentTerm
		newEntry.Index = len(rf.log) + 1

		rf.log = append(rf.log, newEntry)
		rf.newCommand = true
		rf.persist()
		DPrintf("--- add new log entry to [%d] cmd:%d index:%d term:%d time:%v", rf.me, command, newEntry.Index, newEntry.Term, time.Now())
		index = newEntry.Index
		term = newEntry.Term
	}

	return index, term, isLeader
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

// when a server become leader
func (rf *Raft) startLeader() {
	// initialize the the log index
	rf.mu.Lock()
	DPrintf("[%d] begin start leader --- term:%d", rf.me, rf.currentTerm)
	lastIndex := len(rf.log)
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = lastIndex + 1
		rf.matchIndex[i] = 0
	}
	rf.newCommand = false
	rf.leaderReady = true
	rf.isReady.Broadcast()
	rf.mu.Unlock()

	for rf.killed() == false {
		DPrintf("[%d] leader begin send, time:%v", rf.me, time.Now())
		rf.mu.Lock()
		if rf.state != Leader {
			DPrintf("[%d] is not leader", rf.me)
			rf.mu.Unlock()
			return
		}

		replicaLogNum := len(rf.log)
		successNum := 1
		finishNum := 1
		// check if receive new log
		//if rf.newCommand {
		//	// begin log replication
		//	replicaLogNum = rf.newCommandIndex
		//}

		rf.mu.Unlock()
		cond := sync.NewCond(&rf.mu)
		changeFinishLock := sync.Mutex{}

		for i, _ := range rf.peers {
			if i != rf.me {
				go func(x int) {

					rf.mu.Lock()
					if rf.state != Leader {
						rf.mu.Unlock()
						return
					}
					args := AppendEntriesArgs{}
					args.Term = rf.currentTerm
					args.LeaderId = rf.me
					args.LeaderCommit = rf.commitIndex
					reply := AppendEntriesReply{}

					args.PrevLogIndex = rf.nextIndex[x] - 1
					DPrintf("leader[%d]->args[%d].PrevLogIndex:%d", rf.me, x, args.PrevLogIndex)
					if args.PrevLogIndex > 0 && len(rf.log) > 0 {
						args.PrevLogTerm = rf.log[args.PrevLogIndex-1].Term
					}
					// replicalogNum become the length of leader's log
					entries := make([]Entry, replicaLogNum-args.PrevLogIndex)
					copy(entries, rf.log[args.PrevLogIndex:replicaLogNum])
					args.Entries = entries
					rf.mu.Unlock()

					//DPrintf("begin send heartbeat from [%d]-term%d to [%d]",rf.me,rf.currentTerm,x)
					ok := false
					hasChange := false
					tempLock := sync.Mutex{}
					go func() {
						time.Sleep(50 * time.Millisecond)
						tempLock.Lock()
						if ok == false {
							if hasChange == false {
								hasChange = true
								changeFinishLock.Lock()
								finishNum++
								changeFinishLock.Unlock()
								cond.Broadcast()
							}
						}
						tempLock.Unlock()
					}()

					t := rf.peers[x].Call("Raft.AppendEntries", &args, &reply)
					tempLock.Lock()
					ok = t
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if rf.state == Leader && ok && rf.currentTerm == args.Term {
						DPrintf("leader [%d] newCommand:%v receive ok from [%d], reply.MatchIndex:%d, reply.ConfIndex:%d, reply.ConfTerm:%d", rf.me, rf.newCommand, x, reply.MatchIndex, reply.ConfIndex, reply.ConfTerm)
						if reply.Success {
							//DPrintf("[%d] reply success from [%d] matchindex:%d, reply matchindex:%d,replicaLogNum:%d",rf.me,x,rf.matchIndex[x],reply.MatchIndex,replicaLogNum)
							if rf.matchIndex[x] != replicaLogNum && rf.matchIndex[x] < reply.MatchIndex {
								rf.matchIndex[x] = reply.MatchIndex
								rf.nextIndex[x] = reply.MatchIndex + 1
							}
							successNum++
						} else {
							if reply.Term > rf.currentTerm {
								// if receice a newer term, become follower and begin timer
								rf.currentTerm = reply.Term
								rf.state = Follower
								rf.votedFor = -1
								rf.persist()
								if !rf.startTicker {
									rf.startTicker = true
									go rf.ticker()
								}
							}
							// decrement nextIndex
							if reply.ConfTerm != -1 {
								confTermIndex := -1
								for i := args.PrevLogIndex; i >= 1; i-- {
									if rf.log[i-1].Term == reply.ConfTerm {
										confTermIndex = i
										break
									}
									if rf.log[i-1].Term < reply.ConfTerm {
										break
									}
								}
								if confTermIndex == -1 {
									rf.nextIndex[x] = reply.ConfIndex
								} else {
									rf.nextIndex[x] = confTermIndex + 1
								}
							} else if reply.ConfIndex != -1 {
								rf.nextIndex[x] = reply.ConfIndex + 1
							}

							if !rf.newCommand {
								successNum++
							}
						}
					}
					if hasChange == false {
						hasChange = true
						changeFinishLock.Lock()
						finishNum++
						changeFinishLock.Unlock()
						cond.Broadcast()
					}
					tempLock.Unlock()
				}(i)
			}
		}
		rf.mu.Lock()
		for successNum <= len(rf.peers)/2 {
			changeFinishLock.Lock()
			if finishNum != len(rf.peers) {
				changeFinishLock.Unlock()
				cond.Wait()
			} else {
				changeFinishLock.Unlock()
				break
			}
		}
		//DPrintf("[%d] rf.newCommand:%t,rf.successNum:%d,finishNum:%d",rf.me,rf.newCommand,successNum,finishNum)
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}
		if successNum > len(rf.peers)/2 && rf.newCommand {
			// no new log entry added when do log replica
			if replicaLogNum == len(rf.log) {
				rf.newCommand = false
			}
			if replicaLogNum > 0 && rf.log[replicaLogNum-1].Term == rf.currentTerm {
				DPrintf("most peers agree leader[%d] replicaLogNum:%d", rf.me, replicaLogNum)
				rf.commitIndex = replicaLogNum
				rf.applyCond.Broadcast()
			}
			//go rf.applyNewMsg()
		} else {
			if rf.commitIndex != replicaLogNum {
				var tempArray []int
				tempArray = append(tempArray, replicaLogNum)
				for t := 0; t < len(rf.peers); t++ {
					if t != rf.me {
						tempArray = append(tempArray, rf.matchIndex[t])
					}
				}
				sort.Ints(tempArray)
				var mid int
				if len(tempArray)%2 == 0 {
					mid = len(tempArray)/2 - 1
				} else {
					mid = len(tempArray) / 2
				}
				if tempArray[mid] > rf.commitIndex && rf.log[tempArray[mid]-1].Term == rf.currentTerm {
					rf.commitIndex = tempArray[mid]
					rf.applyCond.Broadcast()
				}
			}
		}
		rf.mu.Unlock()
		time.Sleep(50 * time.Millisecond)
	}
}

// apply the newmsg from last applied to commitIndex
func (rf *Raft) applyNewMsg() {
	newMsg := ApplyMsg{}
	rf.mu.Lock()
	tempCommitIndex := rf.commitIndex
	rf.mu.Unlock()
	for i := rf.lastApplied + 1; i <= tempCommitIndex; i++ {
		newMsg.CommandValid = true
		newMsg.CommandIndex = i
		newMsg.Command = rf.log[i-1].Cmd
		rf.applyCh <- newMsg
		DPrintf("[%d] apply a new cmd, index:%d", rf.me, i)
	}
	rf.mu.Lock()
	rf.lastApplied = tempCommitIndex
	rf.mu.Unlock()
}

func (rf *Raft) apply() {
	for rf.killed() == false {
		rf.mu.Lock()
		for rf.lastApplied == rf.commitIndex {
			rf.applyCond.Wait()
		}
		tempCommitIndex := rf.commitIndex
		tempLastApplied := rf.lastApplied
		entries := make([]Entry, tempCommitIndex-tempLastApplied)
		copy(entries, rf.log[tempLastApplied:tempCommitIndex])
		rf.mu.Unlock()
		newMsg := ApplyMsg{}
		for _, i := range entries {
			newMsg.CommandValid = true
			newMsg.CommandIndex = i.Index
			newMsg.Command = i.Cmd
			rf.applyCh <- newMsg
			DPrintf("[%d] apply a new cmd, index:%d, cmd:%d", rf.me, i.Index, i.Cmd)
		}
		rf.mu.Lock()
		rf.lastApplied = tempCommitIndex
		rf.mu.Unlock()
	}
}

// when time out, start state transfer from follow to candidate
func (rf *Raft) startCandidate() {

	// become candidate and send request vote
	rf.mu.Lock()
	if rf.state != Candidate {
		rf.mu.Unlock()
		return
	}
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.persist()

	args := RequestVoteArgs{}
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	if len(rf.log) != 0 {
		args.LastLogIndex = rf.log[len(rf.log)-1].Index
		args.LastLogTerm = rf.log[len(rf.log)-1].Term
	}
	rf.mu.Unlock()
	voteNum := 1
	finishNum := 1
	cond := sync.NewCond(&rf.mu)

	changeFinishLock := sync.Mutex{}

	for i, _ := range rf.peers {
		if i != rf.me {
			go func(x int) {
				reply := RequestVoteReply{}
				//DPrintf("[%d] wait for request vote call to [%d]",rf.me,x)
				ok := false
				hasChange := false
				tempLock := sync.Mutex{}
				go func() {
					time.Sleep(50 * time.Millisecond)
					tempLock.Lock()
					if ok == false {
						if hasChange == false {
							hasChange = true
							changeFinishLock.Lock()
							finishNum++
							changeFinishLock.Unlock()
							cond.Broadcast()
						}
					}
					tempLock.Unlock()
				}()

				t := rf.peers[x].Call("Raft.RequestVote", &args, &reply)
				tempLock.Lock()
				ok = t
				rf.mu.Lock()
				if rf.state == Candidate && ok && !hasChange {
					DPrintf("candidate [%d] receive request vote call from [%d]", rf.me, x)
					if reply.VoteGranted {
						voteNum++
					} else {
						if reply.Term > rf.currentTerm {
							rf.currentTerm = reply.Term
							rf.state = Follower
							rf.persist()
						}
					}
				}
				if hasChange == false {
					hasChange = true
					changeFinishLock.Lock()
					finishNum++
					changeFinishLock.Unlock()
					cond.Broadcast()
				}
				tempLock.Unlock()
				rf.mu.Unlock()
			}(i)
		}
	}
	rf.mu.Lock()
	for voteNum <= len(rf.peers)/2 {
		changeFinishLock.Lock()
		if finishNum != len(rf.peers) {
			changeFinishLock.Unlock()
			cond.Wait()
		} else {
			changeFinishLock.Unlock()
			break
		}
	}
	if rf.state != Candidate {
		rf.mu.Unlock()
		return
	}
	if voteNum > len(rf.peers)/2 {
		//DPrintf("[%d] become leader",rf.me)
		rf.state = Leader
		rf.leaderReady = false
		rf.persist()
		go rf.startLeader()
	}
	rf.mu.Unlock()
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	DPrintf("[%d] start ticker", rf.me)
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.mu.Lock()
		rf.isReceive = false
		rf.mu.Unlock()

		rr, _ := crand.Int(crand.Reader, big.NewInt(150))
		waitTime := 200 + rr.Int64()
		time.Sleep(time.Duration(waitTime) * time.Millisecond)

		rf.mu.Lock()
		if !rf.isReceive {
			if rf.state == Leader {
				rf.startTicker = false
				rf.mu.Unlock()
				return
			}
			rf.state = Candidate
			rf.persist()
			rf.mu.Unlock()
			DPrintf("[%d] begin startcandidate, before term:%d time: %v", rf.me, rf.currentTerm, time.Now())

			go rf.startCandidate()
			continue
		}
		rf.mu.Unlock()
	}
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
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.votedFor = -1

	rf.applyCh = applyCh
	rf.isReady = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	rf.startTicker = true
	go rf.ticker()
	rf.applyCond = sync.NewCond(&rf.mu)
	go rf.apply()
	return rf
}
