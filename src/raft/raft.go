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
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"example.com/6.824/src/labrpc"
)

// import "bytes"
// import "../labgob"

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

type LogEntry struct {
	Command interface{}
	Term    int
}

const (
	LEADER    = "leader"
	FOLLOWER  = "follower"
	CANDIDATE = "canditate"
)

const (
	NULL_VOTE = -1
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu            sync.Mutex          // Lock to protect shared access to this peer's state
	peers         []*labrpc.ClientEnd // RPC end points of all peers
	persister     *Persister          // Object to hold this peer's persisted state
	me            int                 // this peer's index into peers[]
	dead          int32               // set by Kill()
	currentTerm   int                 // latest term the server has seen (persist)
	votedFor      int                 // candidateId that received vote in current election (or NULL_VOTE if none) (persist)
	log           []LogEntry          // log entries of commands for the state machine (persist)
	commitIndex   int                 // index of highest log entry known to be committed
	lastApplied   int                 // index of highest log entry applied to state machine (via ApplyMsg)
	state         string              // whether this server is a leader, follower, or candidate
	applyCh       chan ApplyMsg       // channel server will use to send commands to local state machine
	electionTimer time.Duration       // elapsed time since last heartbeat or election start
	electionCh    chan bool           // channel for election monitor to tell startElection to give up
	nextIndex     []int               // for each peer, index of the next log entry to send
	matchIndex    []int               // for each peer, index of highest log entry known to be replicated on that peer
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == LEADER
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
}

//
// RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	Term         int // candidates term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // Term of candidate's last log entry
}

//
// RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	Term        int  // rf.currentTerm, for the candidate to update itself if necessary
	VoteGranted bool // true -> candidate received vote
}

//
// RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	log.Printf("[%v] received RequestVote from %v\n", rf.me, args.CandidateId)
	reply.VoteGranted = false
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		rf.votedFor = NULL_VOTE
	}
	// if we haven't already voted for someone else and candidate is at least as up-to-date
	if (rf.votedFor == NULL_VOTE || rf.votedFor == args.CandidateId) && (len(rf.log) == 0 ||
		(args.LastLogTerm > rf.log[len(rf.log)-1].Term || args.LastLogIndex > len(rf.log)-1)) {
		log.Printf("[%v] voting for %v\n", rf.me, args.CandidateId)
		reply.VoteGranted = true
	}
}

//
// code to send a RequestVote RPC to a server.
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply,
	resultCh chan RPCInfo) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	resultCh <- RPCInfo{server, ok}
	log.Printf("[%v] returned from sending RequestVote to %v\n", rf.me, server)
	return ok
}

//
// The information startElection needs about the result of the RequestVote or AppendEntries RPC
//
type RPCInfo struct {
	peerIndex int
	ok        bool
}

//
// AppendEntries RPC arguments structure.
//
type AppendEntriesArgs struct {
	Term         int        // leader's term
	LeaderId     int        // so follower can redirect clients (TODO)
	PrevLogIndex int        // index of log entry immediately preceeding new ones
	PrevLogTerm  int        // term of PrevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for heartbeat)
	LeaderCommit int        // leader's commitIndex
}

//
// AppendEntries RPC reply structure.
//
type AppendEntriesReply struct {
	Term    int  // rf.currentTerm, for the leader to update itself if necessary
	Success bool // true -> follower contained entry matching PrevLogIndex and PrevLogTerm
}

//
// AppendEntries RPC
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// log.Printf("[%v] received AppendEntries from %v\n", rf.me, args.LeaderId)
	reply.Success = false
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}

	// check to update term
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	if len(args.Entries) == 0 {
		// heartbeat
		log.Printf("[%v] heartbeat received\n", rf.me)
		// reset election timer
		rf.electionCh <- true
		reply.Success = true
		return
	}

	// TODO check prev info, then reply and possibly append new entries
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply,
	resultCh chan RPCInfo) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	resultCh <- RPCInfo{server, ok}
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

//
// Make this server a follower
// Caller must hold rf.mu
//
func (rf *Raft) becomeFollower(term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	log.Printf("[%v] becoming a follower, updating term to %v\n", rf.me, term)
	if rf.state == CANDIDATE {
		// stop current election
		log.Printf("[%v] stopping current election\n", rf.me)
		rf.electionCh <- true
	}
	rf.currentTerm = term
	rf.state = FOLLOWER
	rf.votedFor = NULL_VOTE
}

//
// Make this server a candidate
// Caller must hold rf.mu
//
func (rf *Raft) becomeCandidate() *RequestVoteArgs {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == CANDIDATE {
		// stop current election
		log.Printf("[%v] stopping current election\n", rf.me)
		rf.electionCh <- true
	}
	// become a candidate
	rf.state = CANDIDATE
	rf.currentTerm++
	rf.votedFor = rf.me
	log.Printf("[%v] becoming a candidate for term %v\n", rf.me, rf.currentTerm)

	// prepare RPC args (same for all calls, read-only, so ok to have just one instance)
	args := RequestVoteArgs{}
	args.CandidateId = rf.me
	if len(rf.log) > 0 {
		args.LastLogIndex = len(rf.log) - 1
		args.LastLogTerm = rf.log[len(rf.log)-1].Term
	}
	args.Term = rf.currentTerm
	return &args
}

//
// Make this server a leader
// Caller must hold rf.mu
//
func (rf *Raft) becomeLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	log.Printf("[%v] becoming the leader for term %v\n", rf.me, rf.currentTerm)
	rf.state = LEADER
	// reset our own election timeout
	rf.electionCh <- true

	// initialize nextIndex
	rf.nextIndex = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = len(rf.log)
	}

	// initialize matchIndex
	rf.matchIndex = make([]int, len(rf.peers))

	go rf.heartbeat()
}

func (rf *Raft) waitForHeartbeats(timer *time.Timer, replys []AppendEntriesReply, replyCh chan RPCInfo) {
	for {
		select {
		case <-timer.C:
			// time to stop waiting on the old heartbeats and send out new ones
			return
		case r := <-replyCh:
			// check if we need to become a follower due to getting a reply with a higher term
			rf.mu.Lock()
			if r.ok && replys[r.peerIndex].Term > rf.currentTerm {
				rf.currentTerm = replys[r.peerIndex].Term
				rf.state = FOLLOWER
				rf.votedFor = NULL_VOTE
			}
			rf.mu.Unlock()
		}
	}
}

//
// Send out heartbeats to peers every 100ms
// (AppendEntries with empty entries)
//
func (rf *Raft) heartbeat() {
	rf.mu.Lock()
	for rf.state == LEADER {
		log.Printf("[%v] sending out heartbeats\n", rf.me)
		rf.electionCh <- true // reset election timer
		args := AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me}
		replys := make([]AppendEntriesReply, len(rf.peers))
		replyCh := make(chan RPCInfo, len(replys))
		rf.mu.Unlock()
		for i := 0; i < len(replys); i++ {
			if i != args.LeaderId {
				go rf.sendAppendEntries(i, &args, &replys[i], replyCh)
			}
		}
		timer := time.NewTimer(100 * time.Millisecond)
		rf.waitForHeartbeats(timer, replys, replyCh)
		rf.mu.Lock()
	}
	rf.mu.Unlock()
	// log.Printf("[%v] stopping heartbeats\n", rf.me)
}

func (rf *Raft) startElection() {
	// request votes in parallel
	args := rf.becomeCandidate()
	replys := make([]RequestVoteReply, len(rf.peers))
	replyCh := make(chan RPCInfo, len(replys))
	for i := 0; i < len(rf.peers); i++ {
		if i != args.CandidateId {
			go rf.sendRequestVote(i, args, &replys[i], replyCh)
		}
	}

	// tally votes
	votes := 1 // we voted for ourselves
	total := 1
	// log.Printf("%v\n", len(rf.peers)/2.0)
	for votes <= len(rf.peers)/2 && total < len(rf.peers) {
		// log.Printf("[%v] waiting on reply or election timeout\n", rf.me)
		select {
		case <-rf.electionCh:
			// timeout is up, end election
			log.Printf("[%v] ending current election due to timeout\n", rf.me)
			// TODO is there state that needs to change here? Or cleanup?
			return
		case rvi := <-replyCh:
			if rvi.ok {
				log.Printf("[%v] tallying %v vote from %v\n", rf.me, replys[rvi.peerIndex].VoteGranted, rvi.peerIndex)
				total++
				if replys[rvi.peerIndex].VoteGranted {
					votes++
				} else if replys[rvi.peerIndex].Term > args.Term {
					rf.becomeFollower(replys[rvi.peerIndex].Term)
					return
				}
			} else {
				// RPC did not succeed, resend
				// log.Printf("[%v] retrying vote request to %v\n", rf.me, rvi.peerIndex)
				go rf.sendRequestVote(rvi.peerIndex, args, &replys[rvi.peerIndex], replyCh)
			}
		}
	}
	if votes > len(rf.peers)/2 {
		rf.becomeLeader()
	}
}

func getElectionTimeout() time.Duration {
	return time.Duration((1500 + rand.Int63n(1000)) * int64(time.Millisecond))
}

//
// thread to monitor election timeout and trigger a new election if necessary
// AppendEntries resets rf.electionTime, startElection launches new electionMonitor
// timeout is in integer milliseconds
//
func (rf *Raft) electionMonitor(timer *time.Timer, reset chan bool) {
	for {
		select {
		case <-reset:
			// log.Printf("[%v] election timer reset\n", rf.me)
			if !timer.Stop() {
				<-timer.C
			}
			timer.Reset(getElectionTimeout())
		case <-timer.C:
			log.Printf("[%v] starting election due to timeout\n", rf.me)
			go rf.startElection()
			timer.Reset(getElectionTimeout())
		}
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
	log.Printf("[%v] making Raft %v\n", me, me)
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh
	rf.votedFor = NULL_VOTE
	rf.state = FOLLOWER
	rf.electionCh = make(chan bool, 1)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.electionMonitor(time.NewTimer(getElectionTimeout()), rf.electionCh)

	return rf
}
