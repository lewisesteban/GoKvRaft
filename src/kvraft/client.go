package raftkv

import "labrpc"
import "crypto/rand"
import (
	"math/big"
	"sync"
)


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leader int
	myID int16
	mu sync.Mutex
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

var lastCmdId int32 = -1
var getCmdIdMu sync.Mutex
func getCmdID() int32 {
	getCmdIdMu.Lock()
	defer getCmdIdMu.Unlock()
	lastCmdId++
	return lastCmdId
}

var lastClientId int16 = -1
var getClientIdMu sync.Mutex
func getClientID() int16 {
	getClientIdMu.Lock()
	defer getClientIdMu.Unlock()
	lastClientId++
	return lastClientId
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {

	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.leader = 0
	ck.myID = getClientID()

	return ck
}

func (ck *Clerk) changeLeader() {
	ck.leader = (ck.leader + 1) % len(ck.servers)
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
	ck.mu.Lock()
	defer ck.mu.Unlock()

	//fmt.Println("### " + strconv.FormatInt(ck.myID, 10) + "\tkey=" + key + "\tget")
	var args = GetArgs{key, getCmdID(), ck.myID}
	for {
		var reply GetReply
		success := ck.servers[ck.leader].Call("KVServer.Get", &args, &reply)
		if success && reply.WrongLeader {
			success = false
		}
		if !success {
			ck.changeLeader()
		} else {
			DPrintf("get end key=" + key + " value=" + reply.Value)
			return reply.Value
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
	ck.mu.Lock()
	defer ck.mu.Unlock()

	var args = PutAppendArgs{key, value, op, getCmdID(), ck.myID}
	var success = false
	for !success {
		var reply PutAppendReply
		success = ck.servers[ck.leader].Call("KVServer.PutAppend", &args, &reply)
		if success && reply.WrongLeader {
			success = false
		}
		if !success {
			ck.changeLeader()
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	//fmt.Println("### " + strconv.FormatInt(ck.myID, 10) + "\tkey=" + key + "\tput " + value)
	ck.PutAppend(key, value, "Put")
	DPrintf("put end key=" + key + " value=" + value)
}

func (ck *Clerk) Append(key string, value string) {
	//fmt.Println("### " + strconv.FormatInt(ck.myID, 10) + "\tkey=" + key + "\tapp " + value)
	ck.PutAppend(key, value, "Append")
	DPrintf("append end key=" + key + " value=" + value)
}
