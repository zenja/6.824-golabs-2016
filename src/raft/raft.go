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

import "sync"
import (
	"labrpc"
	"log"
	"math/rand"
	"time"
)

// import "bytes"
// import "encoding/gob"

type Role int

const (
	Leader Role = iota
	Follower
	Candidate
)

func (r Role) String() string {
	if r == Follower {
		return "Follower"
	}
	if r == Candidate {
		return "Candidate"
	}
	if r == Leader {
		return "Leader"
	}
	panic("Role not recognized!")
}

const heartbeatInterval = 50 * time.Millisecond

var newRand = rand.New(rand.NewSource(time.Now().UnixNano()))

func init() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type LogEntry struct {
	data interface{}
	term int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu sync.Mutex

	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	/* --- Persistent state on all servers --- */
	/* (Updated on stable storage before responding to RPCs) */

	// latest term server has seen (initialized to 0 on first boot, increases monotonically)
	currentTerm int

	// candidateId that received vote in current term (or null if none)
	votedFor int

	// log entries; each entry contains command for state machine, and term when entry
	// was received by leader (first index is 1)
	log []LogEntry

	/* --- Volatile state on all servers --- */

	// index of highest log entry known to be committed (initialized to 0, increases monotonically)
	commitIndex int

	// index of highest log entry applied to state machine (initialized to 0, increases monotonically)
	lastApplied int

	/* --- Volatile state on leaders --- */
	/* (Reinitialized after election) */

	// for each server, index of the next log entry to send to that server
	// (initialized to leader last log index + 1)
	nextIndex []int

	// for each server, index of highest log entry known to be replicated on server
	// (initialized to 0, increases monotonically)
	matchIndex []int

	/* --- Data not in Figure 2. --- */

	// role of the raft peer: Follower, Candidate, or Leader
	role Role

	// resetElectionTimer is used to notify that the election timer should be set
	resetElectionTimer chan bool

	// changedToFollower is used to notify candidate or leader so they can abort their ongoing processing
	// only candidate/leader should care about this chan
	changedToFollower chan bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	return rf.currentTerm, rf.role == Leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term         int // candidate’s term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry
	LastLogTerm  int // term of candidate’s last log entry
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

//
// example RequestVote RPC handler.
// TODO find formal steps in the paper
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// From Figure 2:
	//
	// ALL Servers:
	// * If commitIndex > lastApplied: increment lastApplied, apply
	//   log[lastApplied] to state machine (§5.3)
	// * If RPC request or response contains term T > currentTerm:
	//   set currentTerm = T, convert to follower (§5.1)
	//
	// Receiver implementation:
	// 1. Reply false if term < currentTerm
	// 2. If votedFor is null or candidateId, and candidate’s log is at
	//    least as up-to-date as receiver’s log, grant vote
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	prevRole := rf.role

	// FIXME "all server logic" duplicated code
	if args.Term > rf.currentTerm {
		log.Printf("%s #%d (term %d) received RequestVote RPC from #%d (term %d),"+
			" reset term and changing to follower if it is not follower",
			rf.role, rf.me, rf.currentTerm, args.CandidateId, args.Term)
		// Update term
		rf.currentTerm = args.Term
		// Change role to follower if is candidate/leader
		if rf.role != Follower {
			rf.becomeFollowerUnSafe(args.Term)
		}
	}

	if prevRole == Follower && (rf.votedFor == -1 || rf.votedFor == args.CandidateId) {
		reply.VoteGranted = true
		// When giving vote to other peer, also need to reset follower's timer
		rf.resetElectionTimer <- true
		log.Printf("#%d voted for #%d and reseting timer", rf.me, args.CandidateId)
	}
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
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int        // leader’s term
	LeaderId     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        // leader’s commitIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	// From Figure 2:
	//
	// ALL Servers:
	// * If commitIndex > lastApplied: increment lastApplied, apply
	//   log[lastApplied] to state machine (§5.3)
	// * If RPC request or response contains term T > currentTerm:
	//   set currentTerm = T, convert to follower (§5.1)
	//
	// Receiver implementation:
	// 1. Reply false if term < currentTerm
	// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	// 3. If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it
	// 4. Append any new entries not already in the log
	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		log.Printf("%s #%d (current term %d) received AppendEntries from #%d (term %d), ignored",
			rf.role, rf.me, rf.currentTerm, args.LeaderId, args.Term)
		reply.Success = false
		return
	}

	// FIXME "all server logic" duplicated code
	prevRole := rf.role
	if args.Term > rf.currentTerm {
		log.Printf("%s #%d (term %d) received AppendEntires RPC from #%d (term %d),"+
			" reset term and changing to follower if it is not follower",
			rf.role, rf.me, rf.currentTerm, args.LeaderId, args.Term)
		// Update term
		rf.currentTerm = args.Term
		// Change role to follower if is candidate/leader
		if rf.role != Follower {
			rf.becomeFollowerUnSafe(args.Term)
		}

		reply.Success = true
	}

	switch {
	case prevRole == Follower:
		// TODO is follower special or should be the same as candidate/leader?
		log.Printf("Follower #%d reseting timer due to the AppendEntries msg", rf.me)
		// try send timer reset signal non-blockingly
		select {
		case rf.resetElectionTimer <- true:
		default:
		}

	case prevRole == Candidate:
		// do nothing

	case prevRole == Leader:
		// do nothing
	}
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendHeartbeatsAsync() {
	if rf.role != Leader {
		log.Printf("Only leader can send heartbeats, but #%d! is a %v", rf.me, rf.role)
		return
	}
	for i := range rf.peers {
		go func(peerId int) {
			if peerId == rf.me {
				return
			}
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: -1,  // TODO
				PrevLogTerm:  -1,  // TODO
				Entries:      nil, // TODO
				LeaderCommit: -1,  // TODO
			}
			reply := AppendEntriesReply{}
			if ok := rf.sendAppendEntries(peerId, args, &reply); !ok {
				log.Printf("[*] Failed to call AppendEntries RPC from #%d to #%d", rf.me, peerId)
			}
		}(i)
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != Leader {
		return -1, -1, false
	}

	index := len(rf.log) // FIXME is this correct?
	term := rf.currentTerm
	var isLeader bool

	// start agreement async since we should return immediately
	go func() {

	}()

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
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

	// Your initialization code here.

	// Init data from Figure 2
	rf.currentTerm = 0 // init to 0 according to the paper
	rf.votedFor = -1   // -1 means null, means voted for no one

	// Init data not from Figure 2
	rf.role = Follower // start as a follower
	rf.resetElectionTimer = make(chan bool)
	rf.changedToFollower = make(chan bool)

	// Start handling in background
	log.Printf("Starting #%d as a follower", rf.me)
	go startRaft(rf)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

// ----------------------------------------------------- Handling ------------------------------------------------------

func startRaft(rf *Raft) {
	for {
		switch rf.role {
		case Follower:
			doFollower(rf)
		case Candidate:
			doCandidate(rf)
		case Leader:
			doLeader(rf)
		}
	}
}

func doFollower(rf *Raft) {
	rf.initFollower()

	// From Figure 2:
	// * Respond to RPCs from candidates and leaders
	// * If election timeout elapses without receiving AppendEntries
	//   RPC from current leader or granting vote to candidate: convert to candidate

	timeout := randomElectionTimeout()
	select {
	case <-rf.resetElectionTimer:
		// do nothing so timer is effectively reset in next loop step
		log.Printf("Follower #%d reset its election timeout", rf.me)
	case <-time.After(timeout):
		// Turn into a candidate
		log.Printf("Follower #%d is turning into a candidate due to election timeout after %v.", rf.me, timeout)
		rf.mu.Lock()
		rf.role = Candidate
		rf.mu.Unlock()
		// TODO are there more cased to handle?
	}
}

func doCandidate(rf *Raft) {
	rf.initCandidate()

	// From the paper:
	// On conversion to candidate, start election:
	// > Increment currentTerm
	// > Vote for self
	// > Reset election timer TODO is this needed? how?
	// > Send RequestVote RPCs to all other servers
	// If votes received from majority of servers: become leader
	// If AppendEntries RPC received from new leader: convert to follower
	// If election timeout elapses: start new election

	rf.mu.Lock()
	// Increment currentTerm
	rf.currentTerm += 1
	// Vote for itself
	rf.votedFor = rf.me
	rf.mu.Unlock()

	// Sending RequestVote RPC to all other peers, in parallel
	// TODO do we need to use lock to protect rf here?
	mutex := &sync.Mutex{}
	var nVotes int = 1 // vote for itself, so init to 1
	gotMajority := make(chan bool)
	// TODO should this be protected by rf.mu or not?
	log.Printf("Candidate #%d is sending RequestVote to all other peers...", rf.me)
	for i := range rf.peers {
		go func(peerId int) {
			if peerId == rf.me {
				return
			}
			request := RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: 0, // TODO fill this arg before
				LastLogTerm:  0, // TODO
			}
			reply := RequestVoteReply{}
			if ok := rf.sendRequestVote(peerId, request, &reply); !ok {
				log.Printf("[*] Failed to call RequestVote RPC from #%d to #%d", rf.me, peerId)
			}
			if reply.VoteGranted {
				mutex.Lock()
				nVotes += 1

				// Check if got majority of votes
				if nVotes >= len(rf.peers)/2+1 {
					// fire gotMojority non-blockingly
					select {
					case gotMajority <- true:
					default:
					}
				}
				mutex.Unlock()
			}

			// TODO handle reply.term
		}(i)
	}

	timeout := randomElectionTimeout()
	select {
	case <-gotMajority:
		// Consider itself as leader
		log.Printf("Candidate #%d got majority votes, turning into leader...", rf.me)
		rf.mu.Lock()
		rf.role = Leader
		rf.mu.Unlock()
		// Send initial heartbeats right after it becomes leader
		log.Printf("New leader #%d is sending initial heartbeats to other peers...", rf.me)
		rf.sendHeartbeatsAsync() // TODO should this be protected by rf.mu?

	// When other candidates claimed to be leader effectively
	case <-rf.resetElectionTimer:
		log.Printf("Candidate #%d reset its election timeout", rf.me)
		rf.mu.Lock()
		rf.role = Follower
		rf.mu.Unlock()

	case <-time.After(timeout):
		// do nothing
		log.Printf("Candidate #%d election timeout (%v) with no winner, starting new election...", rf.me, timeout)

	case <-rf.changedToFollower:
		// do nothing to finish current processing

		// TODO are there more cased to handle?
	}
}

func doLeader(rf *Raft) {
	rf.initLeader()

	select {
	// Send heartbeat messages to all other peers in parallel
	// TODO should this be protected by rf.mu or not?
	case <-time.After(heartbeatInterval):
		log.Printf("Leader #%d is sending heartbeats...", rf.me)
		rf.sendHeartbeatsAsync()

	case <-rf.changedToFollower:
		// do nothing to finish current processing
	}
}

// ---------------------------------------------------- Peer Init ------------------------------------------------------
func (rf *Raft) initFollower() {
	select {
	case <-rf.resetElectionTimer:
	default:
	}
	rf.votedFor = -1
}

func (rf *Raft) initCandidate() {
	select {
	case <-rf.changedToFollower:
	default:
	}
	select {
	case <-rf.resetElectionTimer:
	default:
	}
}

func (rf *Raft) initLeader() {
	select {
	case <-rf.changedToFollower:
	default:
	}

	// init nextIndex[]
	initNextIndex := len(rf.log)
	for i := range rf.nextIndex {
		rf.nextIndex[i] = initNextIndex
	}

	// init matchIndex[]
	for i := range rf.matchIndex {
		rf.matchIndex[i] = 0
	}
}
// ------------------------------------------------------- Utils -------------------------------------------------------

// randomElectionTimeout returns a random duration of [150, 300] ms
func randomElectionTimeout() time.Duration {
	return time.Duration((newRand.Intn(151) + 150)) * time.Millisecond
}

// becomeFollowerUnSafe change the role to follower, notify via rf.changedToFollower, and reset rf.votedFor to -1
// it is "unsafe" because it does not use lock; you need to acquire lock before using this func
func (rf *Raft) becomeFollowerUnSafe(newTerm int) {
	if rf.role == Follower {
		panic("Only candidate or leader can become follower!")
	}
	rf.currentTerm = newTerm
	prevRole := rf.role
	rf.role = Follower
	select {
	case rf.changedToFollower <- true:
	default:
	}
	rf.votedFor = -1
	log.Printf("%s #%d changed into follower and reset votedFor to -1", prevRole, rf.me)
}
