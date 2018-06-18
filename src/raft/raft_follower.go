package raft

import (
	"strconv"
)

func (rf* Raft) ApplyLogs() {
	// apply all logs up to commitIndex
	var lastIndex = rf.getLastLogIndex()
	if rf.lastApplied < lastIndex && rf.lastApplied < rf.commitIndex {
		DPrintf(strconv.Itoa(rf.me) + " commit to " + strconv.Itoa(rf.commitIndex))
	}
	for rf.lastApplied < lastIndex && rf.lastApplied < rf.commitIndex {
		var nextLog = rf.getLogAfter(rf.lastApplied)
		if nextLog.Index <= rf.lastApplied {
			DPrintf("ERROR")
		}
		var applyMsg = ApplyMsg{true, nextLog.Command, nextLog.Index}
		DPrintf(strconv.Itoa(rf.me) + " sending " + strconv.Itoa(nextLog.Index) + " to srv")
		rf.applyCh <- applyMsg
		rf.lastApplied = nextLog.Index
	}
}

func (rf* Raft) appendEntries(entries []LogEntry) {
	if entries[0].Index <= rf.commitIndex {
		alreadyCommitted := 0
		for alreadyCommitted + 1 < len(entries) && entries[alreadyCommitted + 1].Index <= rf.commitIndex {
			alreadyCommitted = alreadyCommitted + 1
		}
		entries = entries[(alreadyCommitted + 1):]
	}
	if len(entries) != 0 {
		if entries[0].Index <= rf.getLastLogIndex() {
			rf.truncateLogAfter(entries[0].Index)
			DPrintf(strconv.Itoa(rf.me) + " truncate: newSize=" + strconv.Itoa(len(rf.Log)) + " lastIndex=" + strconv.Itoa(rf.getLastLogIndex()) + " lastTerm=" + strconv.Itoa(rf.getLastLogTerm()))
		}
		rf.Log = append(rf.Log, entries...)
		rf.statePersist()
	}
}

func (rf *Raft) updateCommitIndex(newCommitIndex int) {
	if newCommitIndex > rf.commitIndex {
		if rf.getLastLogIndex() < newCommitIndex {
			rf.commitIndex = rf.getLastLogIndex()
		} else {
			rf.commitIndex = newCommitIndex
		}
	}
	rf.ApplyLogs()
}

func (rf *Raft) entryExists(index int, term int) bool {
	if index <= rf.snapshot.LastEntryIndex && term <= rf.snapshot.LastEntryTerm {
		return true
	}
	if !rf.entryIsIncluded(index) {
		return false
	}
	if index > rf.snapshot.LastEntryIndex && rf.getLog(index).Term != term {
		return false
	}
	return true
}

// THIS IS AN RPC handler
func (rf *Raft) AppendEntriesHandler(data *AppendEntries, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if data.Term >= rf.CurrentTerm {
		rf.waitedTime = 0
	}
	reply.Term = rf.CurrentTerm
	if data.Term < rf.CurrentTerm || !rf.entryExists(data.PrevLogIndex, data.PrevLogTerm) {
		if len(data.Entries) == 0 {
			DPrintf("heartbeat " + strconv.Itoa(rf.me) + " from " + strconv.Itoa(data.Leader) + " refused: prevIndex=" + strconv.Itoa(data.PrevLogIndex) + " prevTerm=" + strconv.Itoa(data.PrevLogTerm) + " term " + strconv.Itoa(data.Term) + " refused: my term " + strconv.Itoa(rf.CurrentTerm) + " myLastLog=" + strconv.Itoa(rf.getLastLogIndex()) + " myLastLogTerm:" + strconv.Itoa(rf.getLastLogTerm()))
		} else {
			DPrintf("append " + strconv.Itoa(rf.me) + " Log " + strconv.Itoa(data.Entries[0].Index) + "=>" + strconv.Itoa(data.Entries[len(data.Entries)-1].Index) + " prevIndex=" + strconv.Itoa(data.PrevLogIndex) + " prevTerm=" + strconv.Itoa(data.PrevLogTerm) + " term " + strconv.Itoa(data.Term) + " from " + strconv.Itoa(data.Leader) + " refused: my term " + strconv.Itoa(rf.CurrentTerm) + " myLastLog=" + strconv.Itoa(rf.getLastLogIndex()) + " myLastLogTerm:" + strconv.Itoa(rf.getLastLogTerm()))
		}
		reply.Status = false
		reply.LastEntryIndex = rf.getLastLogIndex()
		reply.LastEntryTerm = rf.getLastLogTerm()
		reply.ConflictingLogTerm = reply.LastEntryTerm
		if rf.entryIsIncluded(data.PrevLogIndex) && data.PrevLogIndex > rf.snapshot.LastEntryIndex {
			DPrintf(strconv.Itoa(rf.me) + " conflict: log " + strconv.Itoa(data.PrevLogIndex) + " is at term " + strconv.Itoa(rf.getLog(data.PrevLogIndex).Term))
			reply.ConflictingLogTerm = rf.getLog(data.PrevLogIndex).Term
		}
	} else {
		reply.Status = true
		if len(data.Entries) > 0 {
			rf.appendEntries(data.Entries)
			DPrintf("data " + strconv.Itoa(rf.me) + " index " + strconv.Itoa(data.Entries[0].Index) + " => " + strconv.Itoa(data.Entries[len(data.Entries) - 1].Index) + " from " + strconv.Itoa(data.Leader))
		} else {
			DPrintf("heartbeat " + strconv.Itoa(rf.me) + " from " + strconv.Itoa(data.Leader) + " OK")
		}
		rf.updateCommitIndex(data.CommitIndex)
		if data.Term > rf.CurrentTerm || (data.Term == rf.CurrentTerm && rf.state != STATE_FOLLOWER) {
			rf.becomeFollower(data.Term)
		}
	}
}

// THIS IS AN RPC handler
func (rf *Raft) InstallSnapshot(args *SendSnapshotArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term >= rf.CurrentTerm {
		rf.waitedTime = 0
	}
	reply.Term = rf.CurrentTerm
	if args.Term < rf.CurrentTerm {
		DPrintf("snapshot " + strconv.Itoa(rf.me) + " from " + strconv.Itoa(args.Leader) + " refused: term " + strconv.Itoa(args.Term) + " my term " + strconv.Itoa(rf.CurrentTerm) + " myLastLog=" + strconv.Itoa(rf.getLastLogIndex()) + " myLastLogTerm:" + strconv.Itoa(rf.getLastLogTerm()))
		reply.Status = false
		reply.LastEntryIndex = rf.getLastLogIndex()
		reply.LastEntryTerm = rf.getLastLogTerm()
		reply.ConflictingLogTerm = reply.LastEntryTerm
	} else {
		reply.Status = true
		if args.Term > rf.CurrentTerm || (args.Term == rf.CurrentTerm && rf.state != STATE_FOLLOWER) {
			rf.becomeFollower(args.Term)
		}
		rf.snapshotting = true
		rf.receivedSnapshot(args.Ss)
		DPrintf("data " + strconv.Itoa(rf.me) + " snapshot up to " + strconv.Itoa(args.Ss.LastEntryIndex) + " from " + strconv.Itoa(args.Leader))
	}
}

// switch state
func (rf *Raft) becomeFollower(term int) {
	DPrintf("# STATE follower: " + strconv.Itoa(rf.me))
	rf.resetTimeout()
	rf.changeState(STATE_FOLLOWER)
	rf.setTerm(term)
}
