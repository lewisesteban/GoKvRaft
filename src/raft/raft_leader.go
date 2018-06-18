package raft

import (
	"strconv"
)

type AppendEntries struct {
	Entries []LogEntry
	Leader int
	CommitIndex int
	Term int
	PrevLogIndex int
	PrevLogTerm int
}

type AppendEntriesReply struct {
	Status bool
	Term int
	LastEntryIndex int
	LastEntryTerm int
	ConflictingLogTerm int
}

type SendSnapshotArgs struct {
	Leader int
	Term int
	Ss Snapshot
}

func (rf *Raft) majorityHasLog(index int) bool {
	var nbAbove = 0
	for _, serverHighest := range rf.matchIndex {
		if serverHighest >= index {
			nbAbove++
		}
	}
	return nbAbove > len(rf.peers) / 2
}

func (rf *Raft) canSetCommit(newCommitIndex int) bool {
	if rf.majorityHasLog(newCommitIndex) && rf.getLog(newCommitIndex).Term == rf.CurrentTerm {
		return true
	}
	for _, serverHighest := range rf.matchIndex {
		if serverHighest < newCommitIndex {
			return false
		}
	}
	return true
}

func (rf *Raft) incrementCommit() {
	var highest = rf.getLastLogIndex()
	var commitChanged = false
	var newCommitIndex = rf.getLogAfter(rf.commitIndex).Index
	for newCommitIndex <= highest {
		if rf.canSetCommit(newCommitIndex) {
			rf.commitIndex = newCommitIndex
			commitChanged = true
		}
		newCommitIndex = rf.getLogAfter(newCommitIndex).Index
	}
	if commitChanged {
		DPrintf("new commit = " + strconv.Itoa(rf.commitIndex))
		rf.ApplyLogs()
	}
}

// THIS IS AN RPC sender
// locks after each response
func (rf *Raft) sendAppendEntries(server int) {
	rf.replication[server].Lock()
	for rf.alive && rf.state == STATE_LEADER && rf.getLastLogIndex() >= rf.nextIndex[server] {

		rf.mu.Lock() //
		var snapshotStatus = rf.snapshot.LastEntryIndex
		if rf.nextIndex[server] <= rf.snapshot.LastEntryIndex {
			// send snapshot

			rf.mu.Unlock() //
			DPrintf(strconv.Itoa(rf.me) + " sending snapshot with logs up to " + strconv.Itoa(rf.snapshot.LastEntryIndex) + " to peer " + strconv.Itoa(server) + " term " + strconv.Itoa(rf.CurrentTerm))
			var successfullySent= false
			arg := SendSnapshotArgs{rf.me, rf.CurrentTerm, rf.snapshot}
			var reply AppendEntriesReply
			for rf.alive && rf.state == STATE_LEADER && !successfullySent {
				successfullySent = rf.peers[server].Call("Raft.InstallSnapshot", &arg, &reply)
				if !successfullySent {
					DPrintf("+ " + strconv.Itoa(rf.me) + " lost connection with " + strconv.Itoa(server))
				}
			}
			rf.mu.Lock()
			if rf.state != STATE_LEADER {
				rf.mu.Unlock()
				break
			}
			if snapshotStatus != rf.snapshot.LastEntryIndex {
				rf.mu.Unlock()
				break // A snapshot has been created. Abandon and restart at the next heartbeat.
			}
			if reply.Term > rf.CurrentTerm {
				rf.becomeFollower(reply.Term)
				rf.mu.Unlock()
				break
			}
			rf.matchIndex[server] = rf.snapshot.LastEntryIndex
			DPrintf("- replication (snapshot): " + strconv.Itoa(server) + " now has " + strconv.Itoa(rf.snapshot.LastEntryIndex))
			rf.nextIndex[server] = rf.getLogAfter(rf.snapshot.LastEntryIndex).Index
			rf.incrementCommit()
			rf.mu.Unlock()


		} else {
			// send log

			var entries= rf.getLogsFrom(rf.nextIndex[server])
			var prevLog= rf.getLogBefore(entries[0].Index)
			var prevLogTerm= prevLog.Term
			var prevLogIndex= prevLog.Index
			rf.mu.Unlock() //
			DPrintf(strconv.Itoa(rf.me) + " trying to replicate " + strconv.Itoa(rf.nextIndex[server]) + " => " + strconv.Itoa(rf.getLastLogIndex()) + " term " + strconv.Itoa(rf.CurrentTerm) + " to " + strconv.Itoa(server) + ", prev=" + strconv.Itoa(prevLogIndex) + ", prevTerm=" + strconv.Itoa(prevLogTerm))
			var toAppend= AppendEntries{entries, rf.me, rf.commitIndex, rf.CurrentTerm, prevLogIndex, prevLogTerm}
			var successfullySent= false
			var reply AppendEntriesReply
			for rf.alive && rf.state == STATE_LEADER && !successfullySent {
				successfullySent = rf.peers[server].Call("Raft.AppendEntriesHandler", &toAppend, &reply)
				if !successfullySent {
					DPrintf("+ " + strconv.Itoa(rf.me) + " lost connection with " + strconv.Itoa(server))
				}
			}
			rf.mu.Lock()
			if rf.state != STATE_LEADER {
				rf.mu.Unlock()
				break
			}
			if snapshotStatus != rf.snapshot.LastEntryIndex {
				rf.mu.Unlock()
				break // A snapshot has been created. Abandon and restart at the next heartbeat.
			}
			if reply.Term > rf.CurrentTerm {
				rf.becomeFollower(reply.Term)
				rf.mu.Unlock()
				break
			}
			if reply.Status {
				rf.matchIndex[server] = entries[len(entries) - 1].Index
				DPrintf("- replication: " + strconv.Itoa(server) + " now has " + strconv.Itoa(entries[len(entries) - 1].Index))
				rf.nextIndex[server] = rf.getLogAfter(entries[len(entries) - 1].Index).Index
				prevLogTerm = rf.getLogAfter(prevLogIndex).Term
				prevLogIndex = rf.getLogAfter(prevLogIndex).Index
				rf.incrementCommit()
			} else {
				rf.nextIndex[server] = rf.getLogBefore(rf.nextIndex[server]).Index
				prevLogIndex = rf.getLogBefore(rf.nextIndex[server]).Index
				prevLogTerm = rf.getLogBefore(rf.nextIndex[server]).Term
				if prevLogIndex > rf.snapshot.LastEntryIndex {
					if rf.nextIndex[server] > reply.LastEntryIndex && rf.entryIsIncluded(reply.LastEntryIndex) && rf.getLogTerm(reply.LastEntryIndex) == reply.LastEntryTerm {
						// server is simply late => jump to its last log
						var target = rf.getLogAfter(reply.LastEntryIndex).Index
						for prevLogIndex > rf.snapshot.LastEntryIndex && rf.nextIndex[server] > target {
							rf.nextIndex[server] = rf.getLogBefore(rf.nextIndex[server]).Index
							prevLogIndex = rf.getLogBefore(rf.nextIndex[server]).Index
							prevLogTerm = rf.getLogBefore(rf.nextIndex[server]).Term
						}
						DPrintf("replication correction for " + strconv.Itoa(server) + ": jumped to server's last log prevIndex=" + strconv.Itoa(prevLogIndex))
					} else {
						// there is a conflict => jump to beginning of term of conflicting log
						var conflictingTerm = reply.ConflictingLogTerm
						if prevLogTerm < conflictingTerm {
							conflictingTerm = prevLogTerm
						}
						for prevLogIndex > rf.snapshot.LastEntryIndex && prevLogTerm >= conflictingTerm {
							rf.nextIndex[server] = rf.getLogBefore(rf.nextIndex[server]).Index
							prevLogIndex = rf.getLogBefore(rf.nextIndex[server]).Index
							prevLogTerm = rf.getLogBefore(rf.nextIndex[server]).Term
						}
						DPrintf("replication correction for: " + strconv.Itoa(server) + " jumped to term new term=" + strconv.Itoa(rf.getLog(rf.nextIndex[server]).Term) + " nextIndex=" + strconv.Itoa(rf.getLog(rf.nextIndex[server]).Index))
					}
				} else {
					rf.nextIndex[server] = rf.snapshot.LastEntryIndex // snapshot will be sent
				}
			}
			rf.mu.Unlock()
		}
	}
	rf.replication[server].Unlock()
}

// THIS IS AN RPC sender
// locks after response
func (rf* Raft) sendHeartbeatTo(server int) {
	rf.isSendingHeartbeat[server] = true

	var reply AppendEntriesReply
	var successfullySent = false
	// heartbeat checks last Log
	var entries []LogEntry
	var data = AppendEntries{entries, rf.me, rf.commitIndex, rf.CurrentTerm, rf.getLastLogIndex(), rf.getLastLogTerm()}
	for rf.alive && !successfullySent {
		successfullySent = rf.peers[server].Call("Raft.AppendEntriesHandler", &data, &reply)
		if !successfullySent {
			DPrintf("+ " + strconv.Itoa(rf.me) + " lost connection with " + strconv.Itoa(server))
		}
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == STATE_LEADER {
		if reply.Term > rf.CurrentTerm {
			rf.becomeFollower(reply.Term)
		} else if !reply.Status && len(rf.Log) > 0 {
			// if last Log wrong:
			if rf.getLastLogIndex() > reply.LastEntryIndex {
				rf.nextIndex[server] = rf.getLogAfter(reply.LastEntryIndex).Index
			} else {
				rf.nextIndex[server] = rf.getLastLogIndex()
			}
			go rf.sendAppendEntries(server)
		}
	}

	rf.isSendingHeartbeat[server] = false
}

func (rf* Raft) sendHeartbeat() {
	for server := range rf.peers {
		if server != rf.me && !rf.isSendingHeartbeat[server] {
			go rf.sendHeartbeatTo(server)
		}
	}
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	isLeader = rf.state == STATE_LEADER
	if isLeader {
		rf.mu.Lock()
		if isLeader {
			term = rf.CurrentTerm
			index = rf.getLastLogIndex() + 1
			DPrintf("start " + strconv.Itoa(rf.me) + " index=" + strconv.Itoa(index) + " term=" + strconv.Itoa(rf.CurrentTerm) + " value=", command)
			rf.appendEntries([]LogEntry{{rf.CurrentTerm, index, command}})
			rf.matchIndex[rf.me] = index
			// if we still have to wait a long time before the next heartbeat, trigger immediate heartbeat
			if HEARTBEAT_FREQ - rf.waitedTime > HEARTBEAT_FREQ / 4 {
				rf.waitedTime = HEARTBEAT_FREQ + SLEEP
			}
		}
		rf.mu.Unlock()
	} else {
		term = rf.CurrentTerm
		index = rf.getLastLogIndex()
	}

	return index, term, isLeader
}

func (rf *Raft) leaderStateChanged(isLeader bool) {
	if rf.leaderStateChSet {
		rf.leaderStateCh <- isLeader
	}
}

func (rf *Raft) ListenLeadership(leaderStateListener chan bool) {
	rf.leaderStateCh = leaderStateListener
	rf.leaderStateChSet = true
}

// switch state
func (rf *Raft) becomeLeader() {
	DPrintf("# STATE leader: " + strconv.Itoa(rf.me))
	rf.state = STATE_LEADER
	for i := range rf.peers {
		rf.matchIndex[i] = 0
		rf.nextIndex[i] = rf.getLastLogIndex() + 1
	}
	rf.sendHeartbeat()
}
