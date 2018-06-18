package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new Log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the Log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import (
	"labrpc"
	"time"
	"math/rand"
	"strconv"
	"bytes"
	"labgob"
)

// import "bytes"
// import "labgob"

const STATE_LEADER = 0
const STATE_FOLLOWER = 1
const STATE_CANDIDATE = 2

const FOLLOWER_TIMEOUT_MIN = 300
const FOLLOWER_TIMEOUT_MAX = 400
const HEARTBEAT_FREQ = 101
const SLEEP = 10

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Term int
	Index int
	Command interface{}
}

type Snapshot struct {
	LastEntryIndex int
	LastEntryTerm int
	SrvData []byte
}

type ApplySnapshotFunc func([]byte)
type GetSnapshotDataFunc func() []byte

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// persistent
	CurrentTerm int				  // last term this server has seen
	VotedFor    []int 			  // index in peers[] of the candidate this server voted for
	Log         []LogEntry

	snapshot Snapshot

	// volatile
	commitIndex int		// index of the highest Log entry known to be committed
	lastApplied int		// index of the highest Log entry applied to state machine

	alive bool

	state int			// leader, follower, candidate
	timeout int			// election timeout
	waitedTime int		// time counter used for timeout
	applyCh chan ApplyMsg

	leaderStateCh chan bool // a message is sent when this server gets or looses leadership
	leaderStateChSet bool   // whether or not to send messages to the leaderStateCh

	srvApplySnapshot      ApplySnapshotFunc
	srvGetSnapshotData	  GetSnapshotDataFunc
	nbCommittedLogsToKeep int
	snapshotting          bool		// whether the server is currently creating a snapshot
	savingMu              sync.Mutex

	// for candidate
	obtainedVotes int

	// for leaders
	nextIndex []int		// for each server, index of the next Log entry to send to that server
	matchIndex []int	// for each server, index of the highest Log entry known to be replicated on that server

	replication []sync.Mutex 	// locked when the leader is replicating logs to a server
	isSendingHeartbeat []bool   // for each peer, true if the server is sending heartbeats to it
	// the goal of the above is to prevent numerous locked heartbeat goroutines
}

func (rf *Raft) GetState() (int, bool) {

	var term int
	var leader bool

	term = rf.CurrentTerm
	leader = rf.state == STATE_LEADER

	return term, leader
}

func (rf *Raft) GetServerIndex() int {
	return rf.me
}

func (rf *Raft) GetLogSize() int {
	return rf.persister.RaftStateSize()
}

func (rf *Raft) applySnapshot() {

	rf.truncateLogBefore(rf.snapshot.LastEntryIndex)
	if rf.snapshot.LastEntryIndex > rf.commitIndex {
		rf.commitIndex = rf.snapshot.LastEntryIndex
		rf.lastApplied = rf.snapshot.LastEntryIndex
		rf.srvApplySnapshot(rf.snapshot.SrvData)
	}
}

// locks
func (rf *Raft) createSnapshot(uptoIndex int, uptoTerm int, srvData []byte) {
	rf.snapshotting = true
	DPrintf("snapshotting up to " + strconv.Itoa(uptoIndex) + " ... " + strconv.Itoa(rf.me))
	newFirstIndex := uptoIndex + 1
	if newFirstIndex <= (rf.snapshot.LastEntryIndex + 1) {
		DPrintf("snapshot cancelled: out-of-date " + strconv.Itoa(rf.me))
		rf.snapshotting = false
		return
	}
	rf.snapshot = Snapshot{uptoIndex, uptoTerm, srvData}
	rf.applySnapshot()
	rf.snapshotPersist()
	DPrintf("snapshot OK " + strconv.Itoa(rf.me))
}

// Start creation of a snapshot from the outside
/**
 * Starts the creation of a snapshot if one is not already being created.
 * Returns immediately.
 */
func (rf *Raft) RequestCreateSnapshot() {
	if rf.snapshotting == false {
		rf.snapshotting = true
		DPrintf("SNAPSHOT requested server " + strconv.Itoa(rf.me))
		var upto = rf.lastApplied - rf.nbCommittedLogsToKeep
		var data = rf.srvGetSnapshotData()
		go func() {
			rf.mu.Lock()
			rf.createSnapshot(upto, rf.getLogTerm(upto), data)
			rf.mu.Unlock()
		}()
	}
}

// Starts creation of a snapshot based on demand of a peer
func (rf *Raft) receivedSnapshot(snapshot Snapshot) {
	rf.createSnapshot(snapshot.LastEntryIndex, snapshot.LastEntryTerm, snapshot.SrvData)
}

func (rf *Raft) getPersistingState() []byte {

	buffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buffer)
	encoder.Encode(rf.CurrentTerm)
	encoder.Encode(rf.VotedFor)
	encoder.Encode(rf.Log)
	return buffer.Bytes()
}

func (rf *Raft) statePersist() {

	if !rf.snapshotting {
		go func() {
			rf.savingMu.Lock()
			defer rf.savingMu.Unlock()
			rf.persister.SaveRaftState(rf.getPersistingState())
		}()
	}
}

func (rf *Raft) snapshotPersist() {

	go func() {
		rf.savingMu.Lock()
		defer rf.savingMu.Unlock()
		buffer := new(bytes.Buffer)
		encoder := labgob.NewEncoder(buffer)
		encoder.Encode(rf.snapshot)
		snapshot := buffer.Bytes()
		rf.persister.SaveStateAndSnapshot(rf.getPersistingState(), snapshot)
		rf.snapshotting = false
	}()
}

func (rf *Raft) readPersist(data []byte) {

	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	buffer := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(buffer)
	var term int
	var votedFor []int
	var log []LogEntry
	if decoder.Decode(&term) != nil || decoder.Decode(&votedFor) != nil || decoder.Decode(&log) != nil {
		// TODO error
	} else {
		rf.CurrentTerm = term
		rf.VotedFor = votedFor
		rf.Log = log
	}
}

func (rf *Raft) snapshotReadPersist(data []byte) {

	if data == nil || len(data) < 1 {
		rf.snapshot = Snapshot{0, 0, make([]byte, 0)}
		return
	}
	DPrintf(strconv.Itoa(rf.me) + " recovering snapshot")
	buffer := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(buffer)
	var snapshot Snapshot
	if decoder.Decode(&snapshot) != nil {
		// TODO error
	} else {
		rf.snapshot = snapshot
	}
	rf.applySnapshot()
	DPrintf(strconv.Itoa(rf.me) + " recovering snapshot OK " + 	strconv.Itoa(rf.me) + ": snapshotLast=" + strconv.Itoa(rf.snapshot.LastEntryIndex) + " logSize=" + strconv.Itoa(len(rf.Log)) + " logLast=" + strconv.Itoa(rf.getLastLogIndex()))
}

func (rf *Raft) getLogAfter(index int) LogEntry {
	if len(rf.Log) == 0 {
		return LogEntry{rf.CurrentTerm, rf.snapshot.LastEntryIndex + 1, nil}
	}
	if index <= rf.snapshot.LastEntryIndex {
		return rf.Log[0]
	}
	if index - rf.snapshot.LastEntryIndex >= len(rf.Log) {
		return LogEntry{rf.CurrentTerm, index + 1, nil}
	}
	return rf.getLog(index + 1)
}

func (rf *Raft) getLogBefore(index int) LogEntry {
	if index <= (rf.snapshot.LastEntryIndex + 1) {
		return LogEntry{rf.snapshot.LastEntryTerm, rf.snapshot.LastEntryIndex, nil}
	}
	return rf.getLog(index - 1)
}

func (rf *Raft) getLog(index int) LogEntry {
	return rf.Log[index - rf.snapshot.LastEntryIndex - 1]
}

func (rf *Raft) getLogTerm(index int) int {
/*	if index == 0 {
		return 0
	}*/
	if index <= rf.snapshot.LastEntryIndex {
		return rf.snapshot.LastEntryTerm
	}
	return rf.getLog(index).Term
}

func (rf *Raft) getLogsFrom(index int) []LogEntry {
	if index <= rf.snapshot.LastEntryIndex {
		return rf.Log[0:]
	}
	return rf.Log[(index - rf.snapshot.LastEntryIndex - 1):]
}

func (rf *Raft) entryIsIncluded(index int) bool {
	return index - rf.snapshot.LastEntryIndex <= len(rf.Log)
}

// index is also removed
func (rf *Raft) truncateLogAfter(logIndex int) {
	var sliceIndex = len(rf.Log) - 1
	for sliceIndex >= 0 && rf.Log[sliceIndex].Index != logIndex {
		sliceIndex--
	}
	if sliceIndex <= 0 {
		rf.Log = make([]LogEntry, 0)
	} else {
		rf.Log = rf.Log[:sliceIndex]
	}
}

// index is also removed
func (rf *Raft) truncateLogBefore(logIndex int) {
	if logIndex >= rf.getLastLogIndex() {
		rf.Log = make([]LogEntry, 0)
		return
	}
	var sliceIndex = len(rf.Log) - 1
	for sliceIndex >= 0 && rf.Log[sliceIndex].Index != logIndex {
		sliceIndex--
	}
	if sliceIndex < 0 {
		return
	} else if sliceIndex == len(rf.Log) - 1 {
		rf.Log = make([]LogEntry, 0)
	} else {
		rf.Log = rf.Log[sliceIndex+1:]
	}
}

func (rf *Raft) getLastLogIndex() int {
	if len(rf.Log) == 0 {
		return rf.snapshot.LastEntryIndex
	}
	return rf.Log[len(rf.Log) - 1].Index
}

func (rf *Raft) getLastLogTerm() int {
	lastLogTerm := rf.snapshot.LastEntryTerm
	if len(rf.Log) > 0 {
		lastLogTerm = rf.Log[len(rf.Log) - 1].Term
	}
	return lastLogTerm
}

func (rf *Raft) setTerm(term int) {
	if term != rf.CurrentTerm {
		rf.CurrentTerm = term
		for i := range rf.VotedFor {
			if i != len(rf.VotedFor) - 1 {
				rf.VotedFor[i] = rf.VotedFor[i + 1]
			} else {
				rf.VotedFor[i] = -1
			}
		}
	}
	rf.statePersist()
}

func (rf *Raft) resetTimeout() {
	rf.waitedTime = 0
	rf.timeout = FOLLOWER_TIMEOUT_MIN + (rand.Int() % (FOLLOWER_TIMEOUT_MAX - FOLLOWER_TIMEOUT_MIN))
}

func (rf *Raft) changeState(newState int) {
	wasLeader := rf.state == STATE_LEADER
	rf.state = newState
	isLeader := rf.state == STATE_LEADER
	if wasLeader != isLeader {
		rf.leaderStateChanged(isLeader)
	}
}

func (rf *Raft) mainLoop() {
	for rf.alive {
		if rf.state == STATE_LEADER {
			if rf.waitedTime >= HEARTBEAT_FREQ {
				rf.waitedTime = 0
				rf.sendHeartbeat()
			}
		} else {
			rf.mu.Lock()
			if rf.waitedTime > rf.timeout {
				rf.becomeCandidate()
			}
			rf.mu.Unlock()
		}
		time.Sleep(SLEEP * time.Millisecond)
		rf.waitedTime += SLEEP
	}
}

func (rf *Raft) Kill() {
	DPrintf("KILLED " + strconv.Itoa(rf.me))
	rf.alive = false
	time.Sleep(100 * time.Millisecond)
}

func (rf *Raft) launch() {
	go rf.mainLoop()
}

func (rf *Raft) init(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) {

	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rand.Seed(time.Now().UTC().UnixNano())
	rf.CurrentTerm = 0
	rf.VotedFor = make([]int, 20)
	for i := range rf.VotedFor {
		rf.VotedFor[i] = -1
	}
	rf.Log = make([]LogEntry, 0)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.matchIndex = make([]int, len(peers))
	rf.nextIndex = make([]int, len(peers))
	rf.replication = make([]sync.Mutex, len(peers))
	rf.isSendingHeartbeat = make([]bool, len(peers))
	for i := range rf.isSendingHeartbeat {
		rf.isSendingHeartbeat[i] = false
	}
	rf.applyCh = applyCh
	rf.alive = true
	rf.leaderStateChSet = false
	rf.snapshotting = false

	rf.readPersist(persister.ReadRaftState())
	rf.snapshotReadPersist(persister.ReadSnapshot())

	rf.becomeFollower(rf.CurrentTerm)
	rf.launch()
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {

	rf := &Raft{}
	rf.init(peers, me, persister, applyCh)
	return rf
}

func MakeSnapshottingEnabled(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg,
	srvApplyFunc ApplySnapshotFunc, srvGetSnapshotData GetSnapshotDataFunc, nbCommittedLogsToKeep int) *Raft {

	rf := &Raft{}
	rf.srvApplySnapshot = srvApplyFunc
	rf.srvGetSnapshotData = srvGetSnapshotData
	rf.nbCommittedLogsToKeep = nbCommittedLogsToKeep
	rf.init(peers, me, persister, applyCh)
	return rf
}