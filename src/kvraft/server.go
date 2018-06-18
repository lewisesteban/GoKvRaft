package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"strconv"
	"bytes"
)

const Debug = 0

const OP_NONE = int16(-1)
const OP_GET = int16(0)
const OP_PUT = int16(1)
const OP_APPEND = int16(2)

const NB_COMMITTED_LOGS_TO_KEEP = 0

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
	OpId            int32 // id of this op (unique per client)
	ClientId        int16 // id of the client that initiated the op, or -1 if not initiated by client
	CommandedServer int16 // the server through which the operation was initiated
	CommandedTerm	int   // term at which the operation was commanded
	Data            OpData
}

type OpData struct {
	Key string
	Value string
	OpType int16
}

type HistOp struct {
	Type        	int16
	Key         	string
	PrevImg     	string
	ClientId		int16
	ClientPrevOp	int32
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	opsInProgress       []OpWaiter
	data                map[string]string
	lastAppliedClientOp map[int16]int32 // ID of last applied op for each client
	history				[NB_COMMITTED_LOGS_TO_KEEP]HistOp // last applied operations
	historySize			int
	leaderStateCh       chan bool       // a message is sent here every time Raft becomes a leader of becomes a non-leader

	// Your definitions here.
}

type OpWaiter struct {
	Id int32
	Ch chan OpResult
}

type OpResult struct {
	Success bool
	Data OpData
}

func (kv *KVServer) removeWaiter(opId int32) {
	for indexInSlice, opInProgress := range kv.opsInProgress {
		if opInProgress.Id == opId {
			if indexInSlice == len(kv.opsInProgress) - 1 {
				kv.opsInProgress = kv.opsInProgress[:indexInSlice]
			} else {
				kv.opsInProgress = append(kv.opsInProgress[:indexInSlice], kv.opsInProgress[indexInSlice+1:]...)
			}
			break
		}
	}
}

func (kv *KVServer) opAlreadyFinished(opId int32, clientId int16) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	lastAppliedOp, hasEntry := kv.lastAppliedClientOp[clientId]
	return hasEntry && lastAppliedOp == opId
}

// locks
func (kv *KVServer) startOp(opData OpData, opId int32, clientId int16) chan OpResult {
	var ch = make(chan OpResult)
	kv.mu.Lock()
	kv.opsInProgress = append(kv.opsInProgress, OpWaiter{opId, ch})
	kv.mu.Unlock()
	kv.rf.Start(Op{opId, clientId, int16(kv.rf.GetServerIndex()), kv.rf.CurrentTerm, opData})
	return ch
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {

	// Your code here.
	_, isLeader := kv.rf.GetState()
	if isLeader == false {
		reply.WrongLeader = true
	} else {
		DPrintf("OP start " + strconv.Itoa(kv.rf.GetServerIndex()) + " Get key=" + args.Key)
		reply.WrongLeader = false
		var result OpResult
		if kv.opAlreadyFinished(args.CommandID, args.ClientID) {
			kv.mu.Lock()
			result = OpResult{true, OpData{args.Key, kv.data[args.Key], OP_GET}}
			kv.mu.Unlock()
		} else {
			ch := kv.startOp(OpData{args.Key, "", OP_GET}, args.CommandID, args.ClientID)
			result = <-ch
		}
		if result.Success {
			key := result.Data.Key
			reply.Value = result.Data.Value
			reply.Err = ""
			DPrintf("OP end " + strconv.Itoa(kv.rf.GetServerIndex()) + " Get key=" + key + ", val=" + reply.Value)
		} else {
			reply.Value = ""
			reply.Err = "Lost leadership"
			reply.WrongLeader = true
		}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {

	// Your code here.
	_, isLeader := kv.rf.GetState()
	if isLeader == false {
		reply.WrongLeader = true
	} else {
		reply.WrongLeader = false
		DPrintf("OP start " + strconv.Itoa(kv.rf.GetServerIndex()) + " " + args.Op + " key=" + args.Key + " val=" + args.Value)
		opType := OP_PUT
		if args.Op == "Append" {
			opType = OP_APPEND
		}
		var result OpResult
		if kv.opAlreadyFinished(args.CommandID, args.ClientID) {
			result = OpResult{true, OpData{args.Key, args.Value, opType}}
		} else {
			ch := kv.startOp(OpData{args.Key, args.Value, opType}, args.CommandID, args.ClientID)
			result = <-ch
		}
		if result.Success {
			reply.Err = ""
			pair := result.Data
			DPrintf("OP end " + strconv.Itoa(kv.rf.GetServerIndex()) + " " + args.Op + " key=" + pair.Key)
		} else {
			reply.Err = "Lost leadership"
			reply.WrongLeader = true
		}
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

// locks
func (kv *KVServer) endOpInProgress(committedOp Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for _, opInProgress := range kv.opsInProgress {
		if opInProgress.Id == committedOp.OpId {
			opInProgress.Ch <- OpResult{true, committedOp.Data}
			break
		}
	}
	kv.removeWaiter(committedOp.OpId)
}

// locks
func (kv *KVServer) endAllOps() {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for _, opInProgress := range kv.opsInProgress {
		opInProgress.Ch <- OpResult{false, OpData{"", "", OP_NONE}}
	}
	kv.opsInProgress = make([]OpWaiter, 0)
}

func (kv *KVServer) addToHist(committedOp *Op, clientPrevOp int32) {

	if NB_COMMITTED_LOGS_TO_KEEP == 0 {
		return
	}

	histData := ""
	if committedOp.Data.OpType == OP_PUT || committedOp.Data.OpType == OP_APPEND {
		histData = kv.data[committedOp.Data.Key]
	}
	histOp := HistOp{committedOp.Data.OpType, committedOp.Data.Key, histData, committedOp.ClientId, clientPrevOp}

	if kv.historySize < NB_COMMITTED_LOGS_TO_KEEP {
		kv.history[kv.historySize] = histOp
		kv.historySize++
	} else {
		for i := 1; i < kv.historySize; i++ {
			kv.history[i - 1] = kv.history[i]
		}
		kv.history[kv.historySize - 1] = histOp
	}
}

// locks
func (kv *KVServer) applyToDb(committedOp *Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if committedOp.Data.OpType == OP_GET {
		committedOp.Data.Value = kv.data[committedOp.Data.Key]
	}

	// check double op
	var clientPrevOp int32 = -1
	if committedOp.ClientId != -1 {
		if lastApplied, exists := kv.lastAppliedClientOp[committedOp.ClientId]; exists && lastApplied == committedOp.OpId {
			return
		}
		clientPrevOp = kv.lastAppliedClientOp[committedOp.ClientId]
		kv.lastAppliedClientOp[committedOp.ClientId] = committedOp.OpId
	}

	// apply to db
	kv.addToHist(committedOp, clientPrevOp)
	data := committedOp.Data
	if data.OpType == OP_PUT {
		kv.data[data.Key] = data.Value
	} else if data.OpType == OP_APPEND {
		_, hasValue := kv.data[data.Key]
		if hasValue {
			kv.data[data.Key] += data.Value
		} else {
			kv.data[data.Key] = data.Value
		}
	}
}

func (kv *KVServer) getOldData() (map[string]string, map[int16]int32) {

	data := make(map[string]string)
	for k, v := range kv.data {
		data[k] = v
	}
	for i := kv.historySize - 1; i >= 0; i-- {
		if kv.history[i].Type != OP_GET {
			data[kv.history[i].Key] = kv.history[i].PrevImg
		}
	}

	lastAppliedClientOp := make(map[int16]int32)
	for k, v := range kv.lastAppliedClientOp {
		lastAppliedClientOp[k] = v
	}
	for i := kv.historySize - 1; i >= 0; i-- {
		lastAppliedClientOp[kv.history[i].ClientId] = kv.history[i].ClientPrevOp
	}

	return data, lastAppliedClientOp
}

func snapshotDataToBytes(data1 map[string]string, data2 map[int16]int32) []byte {
	buffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buffer)
	encoder.Encode(data1)
	encoder.Encode(data2)
	return buffer.Bytes()
}

func bytesToSnapshotData(dataBytes []byte) (map[string]string, map[int16]int32) {
	buffer := bytes.NewBuffer(dataBytes)
	decoder := labgob.NewDecoder(buffer)
	var data1 map[string]string
	var data2 map[int16]int32
	if decoder.Decode(&data1) != nil || decoder.Decode(&data2) != nil {
		// TODO error
	}
	return data1, data2
}

// locks
func (kv *KVServer) applySnapshot(bytesData []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	data, clientsLastOps := bytesToSnapshotData(bytesData)
	kv.data = data
	kv.lastAppliedClientOp = clientsLastOps
}

func (kv *KVServer) getSnapshotData() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return snapshotDataToBytes(kv.data, kv.lastAppliedClientOp)
}

func (kv *KVServer) observeCommitment() {
	for {
		logEntry := <-kv.applyCh
		committedOp := logEntry.Command.(Op)
		kv.applyToDb(&committedOp)
		if committedOp.CommandedServer == int16(kv.rf.GetServerIndex()) && committedOp.CommandedTerm == kv.rf.CurrentTerm {
			kv.endOpInProgress(committedOp)
		}
		if kv.maxraftstate != -1 && kv.rf.GetLogSize() >= kv.maxraftstate {
			kv.rf.RequestCreateSnapshot()
		}
	}
}

func (kv *KVServer) observeLeadership() {
	for {
		isLeader := <- kv.leaderStateCh
		if !isLeader {
			DPrintf("server " + strconv.Itoa(kv.me) + " lost leadership")
			kv.endAllOps()
		}
	}
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
	kv.data = make(map[string]string)
	kv.lastAppliedClientOp = make(map[int16]int32)
	kv.leaderStateCh = make(chan bool)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.opsInProgress = make([]OpWaiter, 0)

	go kv.observeCommitment()
	go kv.observeLeadership()

	if maxraftstate == -1 {
		kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	} else {
		kv.rf = raft.MakeSnapshottingEnabled(servers, me, persister, kv.applyCh, kv.applySnapshot,
			func() []byte { return kv.getSnapshotData() }, NB_COMMITTED_LOGS_TO_KEEP)
	}
	kv.rf.ListenLeadership(kv.leaderStateCh)
	// You may need initialization code here.

	return kv
}
