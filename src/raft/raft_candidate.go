package raft

import (
	"strconv"
)

type RequestVoteArgs struct {
	Term int			// candidate term
	CandidateId int		// candidate requesting vote
	LastLogIndex int	// index of candidate's last Log entry
	LastLogTerm int		// term of candidate's last Log entry
}

type RequestVoteReply struct {
	Term int			// current term, for candidate to update itself
	VoteGranted bool
}

func (rf *Raft) getVotedFor(term int) int {
	diff := term - rf.CurrentTerm
	return rf.VotedFor[diff]
}

func (rf *Raft) setVotedFor(term int, value int) {
	diff := term - rf.CurrentTerm
	rf.VotedFor[diff] = value
	rf.statePersist()
}

func (rf *Raft) moreUpToDate(term1 int, index1 int, term2 int, index2 int) bool {
	if term1 > term2 {
		return true
	}
	if term1 == term2 && index1 > index2 {
		return true
	}
	return false
}

// THIS IS AN RPC handler
// locks
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.CurrentTerm
	if args.Term < rf.CurrentTerm {
		DPrintf("denied " + strconv.Itoa(rf.me) + " => " + strconv.Itoa(args.CandidateId) + " (term lower)")
		reply.VoteGranted = false
	} else {
		if args.Term > rf.CurrentTerm {
			rf.becomeFollower(args.Term)
		}
		votedFor := rf.getVotedFor(args.Term)
		if (votedFor == -1 || votedFor == args.CandidateId) && !rf.moreUpToDate(rf.getLastLogTerm(), rf.getLastLogIndex(), args.LastLogTerm, args.LastLogIndex) {
			reply.VoteGranted = true
			rf.setVotedFor(args.Term, args.CandidateId)
			DPrintf("vote " + strconv.Itoa(rf.me) + " => " + strconv.Itoa(args.CandidateId))
		} else {
			if votedFor == -1 || votedFor == args.CandidateId {
				DPrintf("denied " + strconv.Itoa(rf.me) + " => " + strconv.Itoa(args.CandidateId) + " (Log out of date: myLastLog " + strconv.Itoa(rf.getLastLogIndex()) + " leaderLastLog " + strconv.Itoa(args.LastLogIndex) + " myLastLogTerm " + strconv.Itoa(rf.getLastLogTerm()) + " leaderLastLogTerm " + strconv.Itoa(args.LastLogTerm) + " )")
			} else {
				DPrintf("denied " + strconv.Itoa(rf.me) + " => " + strconv.Itoa(args.CandidateId) + " (already voted, candidate term  " + strconv.Itoa(args.Term) + " voter term " + strconv.Itoa(rf.CurrentTerm) + ")")
			}
			reply.VoteGranted = false
		}
	}
}

// THIS IS AN RPC sender
// locks after response
func (rf *Raft) sendRequestVote(server int) bool { // originally had *args and *reply as arguments
	args := RequestVoteArgs{rf.CurrentTerm, rf.me, rf.getLastLogIndex(), rf.getLastLogTerm()}
	var reply RequestVoteReply
	DPrintf(strconv.Itoa(rf.me) + " requests " + strconv.Itoa(server))
	ok := rf.peers[server].Call("Raft.RequestVote", &args, &reply)
	rf.mu.Lock()
	if ok && reply.Term > rf.CurrentTerm {
		rf.becomeFollower(reply.Term)
	}
	rf.mu.Unlock()
	return ok && reply.VoteGranted
}

// locks after return from sendRequestVote
func (rf *Raft) getVote(server int) {
	if rf.sendRequestVote(server) {
		rf.mu.Lock()
		if rf.state == STATE_CANDIDATE { // make sure we're still a candidate
			rf.obtainedVotes++
			if rf.obtainedVotes > (len(rf.peers) / 2) {
				rf.becomeLeader()
			}
		}
		rf.mu.Unlock()
	}
}

// switch state
func (rf *Raft) becomeCandidate() {
	DPrintf("# STATE candidate: " + strconv.Itoa(rf.me))
	rf.resetTimeout()
	rf.setTerm(rf.CurrentTerm + 1)
	rf.changeState(STATE_CANDIDATE)
	rf.obtainedVotes = 1
	rf.setVotedFor(rf.CurrentTerm, rf.me)
	if rf.obtainedVotes > (len(rf.peers) / 2) {
		rf.becomeLeader()
		return
	}
	for server := range rf.peers {
		if server != rf.me {
			go rf.getVote(server)
		}
	}
}
