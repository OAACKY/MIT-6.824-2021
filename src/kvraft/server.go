package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

type OpType int

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

// define OpType
const (
	GetType OpType = iota
	PutType
	AppendType
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Optype OpType
	Cid    int64
	SeNum  int64
	Key    string
	Value  string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	KV         map[string]string
	lastSerial map[int64]int64
	leaderGet  map[int]chan Op

	lastApplied int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("[%d] kvServer receive Get, key:%s", kv.me, args.Key)
	op := Op{}
	op.Optype = GetType
	op.Key = args.Key
	op.Cid = args.ClientId
	op.SeNum = args.SerialNum

	opIndex, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("[%d] leader kvServer receive Get, key:%s cid:%d seNum:%d", kv.me, args.Key, args.ClientId, args.SerialNum)
	kv.mu.Lock()
	waitCh, ok := kv.leaderGet[opIndex]
	if !ok {
		kv.leaderGet[opIndex] = make(chan Op, 1)
		waitCh = kv.leaderGet[opIndex]
	}
	kv.mu.Unlock()

	timeOut := time.Tick(500 * time.Millisecond)
	select {
	case <-timeOut:
		DPrintf("[%d] has timeOut cid:%d seNum:%d", kv.me, args.ClientId, args.SerialNum)
		reply.Err = ErrWrongLeader
	case res := <-waitCh:
		DPrintf("[%d] leader has receive waitCh op.Cid:%d, op.SeNum:%d", kv.me, res.Cid, res.SeNum)
		if res.Cid == op.Cid && res.SeNum == op.SeNum {
			DPrintf("[%d] leader has receive waitCh op.Cid:%d, op.SeNum:%d ok!", kv.me, res.Cid, res.SeNum)
			switch res.Optype {
			case GetType:
				kv.mu.Lock()
				if kv.KV[res.Key] == "" {
					reply.Err = ErrNoKey
				} else {
					reply.Err = OK
				}
				reply.Value = kv.KV[res.Key]
				kv.mu.Unlock()
			default:
				panic("wrong! should not be a Put&Append!")
			}
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}
	}
	DPrintf("get here?")
	kv.mu.Lock()
	DPrintf("get here!")
	closeChan, ok := kv.leaderGet[opIndex]
	if ok {
		close(closeChan)
		delete(kv.leaderGet, opIndex)
	}
	DPrintf("get here!!!?")
	kv.mu.Unlock()
	return
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("[%d] kvServer receive PutAppend,key: %s, value: %s", kv.me, args.Key, args.Value)
	op := Op{}
	op.Key = args.Key
	op.Value = args.Value
	op.Cid = args.ClientId
	op.SeNum = args.SerialNum

	if args.Op == "Put" {
		op.Optype = PutType
	} else {
		op.Optype = AppendType
	}

	opIndex, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("[%d] leader kvServer receive PutAppend,key: %s, value: %s cid:%d seNum:%d", kv.me, args.Key, args.Value, args.ClientId, args.SerialNum)
	kv.mu.Lock()
	waitCh, ok := kv.leaderGet[opIndex]
	if !ok {
		DPrintf("[%d] leader create chan cmd.index:%d", kv.me, opIndex)
		kv.leaderGet[opIndex] = make(chan Op, 1)
		waitCh = kv.leaderGet[opIndex]
	}
	kv.mu.Unlock()

	timeOut := time.Tick(500 * time.Millisecond)
	select {
	case <-timeOut:
		DPrintf("[%d] has timeOut cid:%d seNum:%d", kv.me, args.ClientId, args.SerialNum)
		reply.Err = ErrWrongLeader
	case res := <-waitCh:
		DPrintf("[%d] leader has receive waitCh op.Cid:%d, op.SeNum:%d", kv.me, res.Cid, res.SeNum)
		if res.Cid == op.Cid && res.SeNum == op.SeNum {
			DPrintf("[%d] leader has receive waitCh op.Cid:%d, op.SeNum:%d ok!", kv.me, res.Cid, res.SeNum)
			if res.Optype == GetType {
				panic("wrong! should not be a Get!")
			}
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}
	}
	DPrintf("get here?")
	kv.mu.Lock()
	DPrintf("get here!")
	closeChan, ok := kv.leaderGet[opIndex]
	if ok {
		close(closeChan)
		delete(kv.leaderGet, opIndex)
	}
	DPrintf("get here!!!")
	kv.mu.Unlock()
	return

}

// start a goroutine to check whether a log is applied by raft
func (kv *KVServer) CheckApply() {
	for kv.killed() == false {
		applyMsg := <-kv.applyCh
		DPrintf("[%d] receive applyMsg", kv.me)
		if applyMsg.CommandValid {
			// apply the cmd
			if applyMsg.CommandIndex <= kv.lastApplied {
				continue
			}
			op := applyMsg.Command.(Op)
			kv.mu.Lock()
			DPrintf("[%d] receive applyMsg op.cid:%d op.seNum:%d applyMsg.cmdIndex:%d kv.lastSerial[op.cid]:%d", kv.me, op.Cid, op.SeNum, applyMsg.CommandIndex, kv.lastSerial[op.Cid])
			if kv.lastSerial[op.Cid] < op.SeNum {
				// check if duplicate
				kv.lastSerial[op.Cid] = op.SeNum
				switch op.Optype {
				case GetType:
					break
				case PutType:
					kv.KV[op.Key] = op.Value
				case AppendType:
					kv.KV[op.Key] += op.Value
				}
			}

			// check raftstate size
			if kv.rf.Persister.RaftStateSize() > kv.maxraftstate && kv.maxraftstate != -1 {
				DPrintf("[%d] kvserver has call the snapshot", kv.me)
				kv.rf.Snapshot(applyMsg.CommandIndex, kv.encodeState())
			}

			// check if it's leader
			waitCh, ok := kv.leaderGet[applyMsg.CommandIndex]
			if ok {
				DPrintf("[%d] leader has put in waitChan op.cid:%d op.seNum:%d", kv.me, op.Cid, op.SeNum)
				waitCh <- op
			}
			kv.mu.Unlock()
		} else {
			// if it's snapshot
			kv.mu.Lock()
			DPrintf("[%d] server begin ask condInstallSnapShot", kv.me)
			if kv.rf.CondInstallSnapshot(applyMsg.SnapshotTerm, applyMsg.SnapshotIndex, applyMsg.Snapshot) {
				// install this snapshot
				kv.restoreState(kv.rf.Persister.ReadSnapshot())
				kv.lastApplied = applyMsg.SnapshotIndex
			}
			kv.mu.Unlock()
		}

	}
}

// encode the latest kvserver state
func (kv *KVServer) encodeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(kv.KV) != nil || e.Encode(kv.lastSerial) != nil {
		DPrintf("kvServer encode error!")
	}
	data := w.Bytes()
	return data
}

func (kv *KVServer) restoreState(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	DPrintf("[%d] server begin restoreState", kv.me)
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var tempKV map[string]string
	var tempLastSerial map[int64]int64
	if d.Decode(&tempKV) != nil || d.Decode(&tempLastSerial) != nil {
		DPrintf("kvServer decode error!")
	} else {
		kv.KV = tempKV
		kv.lastSerial = tempLastSerial
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
	kv.KV = make(map[string]string)
	kv.lastSerial = make(map[int64]int64)
	kv.leaderGet = make(map[int]chan Op)

	kv.restoreState(persister.ReadSnapshot())

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go kv.CheckApply()

	return kv
}
