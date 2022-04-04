package kvraft

import "6.824/labrpc"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	lastRPC       int
	clientId      int64
	lastSerialNum int64
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
	// You'll have to add code here.
	ck.lastRPC = -1
	ck.clientId = nrand()
	ck.lastSerialNum = 0
	return ck
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
	ck.lastSerialNum++

	args := GetArgs{}
	reply := GetReply{}
	args.Key = key
	args.ClientId = ck.clientId
	args.SerialNum = ck.lastSerialNum

	res := ""

	DPrintf("client[%d] begin send Get,key: %s", ck.clientId, key)
	if ck.lastRPC != -1 {
		ok := ck.servers[ck.lastRPC].Call("KVServer.Get", &args, &reply)
		if ok && reply.Err != ErrWrongLeader {
			res = reply.Value
			return res
		}
	}
	for {
		for i := 0; i < len(ck.servers); {
			ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
			if !ok || reply.Err == ErrWrongLeader {
				i++
				continue
			}
			ck.lastRPC = i
			res = reply.Value
			return res
		}
	}
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
	ck.lastSerialNum++

	args := PutAppendArgs{}
	reply := PutAppendReply{}
	args.Key = key
	args.Value = value
	args.Op = op
	args.ClientId = ck.clientId
	args.SerialNum = ck.lastSerialNum

	DPrintf("client[%d] begin send PutAppend,key: %s, value: %s", ck.clientId, key, value)
	if ck.lastRPC != -1 {
		ok := ck.servers[ck.lastRPC].Call("KVServer.PutAppend", &args, &reply)
		if ok && reply.Err == OK {
			return
		}
	}
	for {
		for i := 0; i < len(ck.servers); {
			ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
			if !ok || reply.Err == ErrWrongLeader {
				i++
				continue
			}
			if reply.Err == OK {
				ck.lastRPC = i
				return
			}
			i++
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
