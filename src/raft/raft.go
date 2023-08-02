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

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

const (
	Follower  int32 = 0
	Candidate int32 = 1
	Leader    int32 = 2
)

type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	lastReceiveTime time.Time
	state           atomic.Int32
	currentTerm     atomic.Int32
	votedFor        atomic.Int32
	electionTimeOut atomic.Int32 //ms

	log         []LogEntry
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int
	mu3         sync.Mutex

	mu4  sync.Mutex
	cond *sync.Cond

	mu5 sync.Mutex
	//论在这里使用atomic是否有必要

	applyCh chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = int(rf.currentTerm.Load())
	isleader = rf.state.Load() == Leader
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term          int32
	PleaseVoteFor int
	LastLogIndex  int
	LastlogTerm   int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Voted bool
	Term  int32
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	if args.Term > rf.currentTerm.Load() || args.PleaseVoteFor == int(rf.votedFor.Load()) {
		rf.mu3.Lock()
		if args.LastLogIndex >= rf.commitIndex && args.LastlogTerm >= rf.log[rf.commitIndex].Term {
			//if true {
			rf.votedFor.Store(int32(args.PleaseVoteFor))
			rf.refreshElectionTimeout()

			rf.mu.Lock()
			rf.lastReceiveTime = time.Now()
			rf.mu.Unlock()

			rf.currentTerm.Store(args.Term)
			reply.Voted = true
			reply.Term = rf.currentTerm.Load()
			//log.Printf("server %d vote for %d", rf.me, args.PleaseVoteFor)
		} else {
			rf.currentTerm.Store(args.Term)
			reply.Term = rf.currentTerm.Load()
			//log.Printf("server %d refuse to vote for %d", rf.me, args.PleaseVoteFor)
			//log.Printf("raft state: %d, %d 的 LastlogIndex: %d, 当前commitIndex: %d\n--------------------------\n LastlogTerm: %d, 当前提交的最大term: %d", rf.state.Load(), args.PleaseVoteFor, args.LastLogIndex, rf.commitIndex, args.LastlogTerm, rf.log[rf.commitIndex].Term)
			//log.Printf("当前log, %v", rf.log)
		}
		rf.mu3.Unlock()
	} else {
		//log.Printf("server %d refuse to vote for %d", rf.me, args.PleaseVoteFor)
		//log.Printf("raft state: %d, args term: %d, current term: %d, voted for: %d", rf.state.Load(), args.Term, rf.currentTerm.Load(), rf.votedFor.Load())
		reply.Term = rf.currentTerm.Load()
	}
	//if args.Term < rf.currentTerm.Load() {
	//	//log.Printf("server %d refuse to vote for %d", rf.me, args.PleaseVoteFor)
	//	//log.Printf("raft state: %d, args term: %d, current term: %d, voted for: %d", rf.state.Load(), args.Term, rf.currentTerm.Load(), rf.votedFor.Load())
	//	reply.Voted = false
	//	reply.Term = rf.currentTerm.Load()
	//} else if (rf.votedFor.Load() == -1 || rf.votedFor.Load() == int32(args.PleaseVoteFor)) && args.LastLogIndex >= len(rf.log)-1 {
	//
	//}

}

// heartbeatRequest is same as appendEntriesRequest
type AppendEntriesArgs struct {
	Term         int32      //领导人的任期
	PrevLogIndex int        //新的日志条目紧随之前的索引值
	PrevLogTerm  int        //prevLogIndex 条目的任期号
	Entries      []LogEntry //准备存储的日志条目（表示心跳时为空）
	LeaderCommit int        //领导人的已知已提交的最高的日志条目的索引
}

type AppendEntriesReply struct {
	Term    int32
	Success bool
}

func (rf *Raft) AppendEntriesRequest(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu5.Lock()
	defer func() {
		rf.mu5.Unlock()
		if err := recover(); err != nil {
			//log.Printf("server %d panic,args: %v,当前log: %v, panic:%v", rf.me, args, rf.log, err)
			panic(err)
		}
	}()
	reply.Success = true
	if args.Entries == nil { //心跳请求

		if args.Term < rf.currentTerm.Load() {
			//log.Printf("Server %d 拒绝了leader的心跳请求，leader term: %d, current term: %d, state: %d", rf.me, args.Term, rf.currentTerm.Load(), rf.state.Load())
			reply.Term = rf.currentTerm.Load()
			//reply.Success = true
		} else {
			rf.mu.Lock()
			rf.lastReceiveTime = time.Now()
			rf.mu.Unlock()
			rf.refreshElectionTimeout()
			rf.currentTerm.Store(args.Term)
			rf.state.Store(Follower)
			reply.Term = args.Term
			//reply.Success = true
			//log.Printf("server %d receive heartbeat from leader,term更新为 %d", rf.me, args.Term)
			if args.LeaderCommit > rf.commitIndex {
				if len(rf.log) > args.PrevLogIndex && rf.log[args.PrevLogIndex].Term == args.PrevLogTerm {
					for ; rf.commitIndex < min(args.LeaderCommit, len(rf.log)-1); rf.commitIndex++ {
						//log.Printf("server %d apply %d", rf.me, rf.commitIndex+1)
						//if rf.log[rf.commitIndex+1].Term != int(rf.currentTerm.Load()) { //如果该条目的任期不等于当前任期，说明该条目是旧的，不应该被apply
						//	reply.Success = false
						//	break
						//}
						rf.applyCh <- ApplyMsg{CommandValid: true, Command: rf.log[rf.commitIndex+1].Command, CommandIndex: rf.commitIndex + 1}
					}
					//log.Printf("server %d commitIndex更新为 %d", rf.me, rf.commitIndex)
					//if reply.Success == false {
					//	//log.Printf("commitIndex更新失败, 过时的日志")
					//}
					//log.Printf("server %d commitIndex更新为 %d, 此时的logs: %v", rf.me, rf.commitIndex, rf.log)
				} else { //日志不匹配
					reply.Term = rf.currentTerm.Load()
					reply.Success = false
				}

			}
		}
	} else { //日志请求
		if args.Term < rf.currentTerm.Load() {
			//log.Printf("拒绝了leader的AppendEntries请求,term过低,leader term: %d, current term: %d, state: %d", args.Term, rf.currentTerm.Load(), rf.state.Load())
			reply.Term = rf.currentTerm.Load()
			//reply.Success = false
		} else {
			rf.mu3.Lock()
			ok := len(rf.log) > args.PrevLogIndex && rf.log[args.PrevLogIndex].Term == args.PrevLogTerm
			rf.mu3.Unlock()
			if ok { //返回假 如果接收者日志中没有包含这样一个条目 即该条目的任期在 prevLogIndex 上能和 prevLogTerm 匹配上
				rf.mu.Lock()
				rf.lastReceiveTime = time.Now()
				rf.mu.Unlock()
				rf.refreshElectionTimeout()
				rf.currentTerm.Store(args.Term)
				rf.state.Store(Follower)
				reply.Term = args.Term
				//reply.Success = true
				rf.mu3.Lock()
				rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)

				//log.Printf("server %d 接受 appendEntries from leader,term更新为 %d,log数量: %d", rf.me, args.Term, len(rf.log)-1)
				//log.Printf("server %d 的当前log: %v", rf.me, rf.log)
				rf.mu3.Unlock()
			} else { //日志不匹配
				reply.Term = rf.currentTerm.Load()
				reply.Success = false
			}

		}
	}

}

// example code to send a RequestVote RPC to a server.
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntriesRequest(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntriesRequest", args, reply)
	return ok
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu3.Lock()
	isLeader = rf.state.Load() == Leader
	index = len(rf.log)
	term = int(rf.currentTerm.Load())
	rf.mu3.Unlock()
	if isLeader {
		rf.mu3.Lock()
		rf.log = append(rf.log, LogEntry{Term: int(rf.currentTerm.Load()), Index: len(rf.log), Command: command})
		//log.Printf("leader %d 收到客户端请求,leader %d append command %d", rf.me, rf.me, len(rf.log)-1)
		rf.mu3.Unlock()
		rf.cond.Signal()

	}

	return index, term, isLeader
}

func (rf *Raft) broadcastAppendEntries() {

	count := 1
	mu2 := sync.Mutex{}
	cond := sync.NewCond(&mu2)

	rf.mu3.Lock()
	//log.Printf("leader %d 开始broadcastAppendEntries, 当前log数量: %d, 当前leaderCommit: %d", rf.me, len(rf.log)-1, rf.commitIndex)
	//term := rf.currentTerm.Load()
	////entries := rf.log[rf.nextIndex[rf.me]:]
	////prevLogIndex := rf.nextIndex[rf.me] - 1
	////prevLogTerm := rf.log[rf.nextIndex[rf.me]-1].Term
	logs := rf.log
	commitIndex := rf.commitIndex
	logsLen := len(rf.log)
	rf.mu3.Unlock()
	//currentTerm := rf.currentTerm.Load()
	for i := range rf.peers {
		if i == rf.me {
			continue
		} else {
			go func(index int) {
				rf.mu3.Lock()
				args := AppendEntriesArgs{
					Term:         rf.currentTerm.Load(),
					Entries:      logs[rf.nextIndex[index]:],
					PrevLogIndex: rf.nextIndex[index] - 1,
					PrevLogTerm:  logs[rf.nextIndex[index]-1].Term,
					LeaderCommit: commitIndex,
				}

				rf.mu3.Unlock()
				var reply AppendEntriesReply
				for reply.Success == false && rf.state.Load() == Leader {
					reply = AppendEntriesReply{}
					if len(args.Entries) == 0 {
						//log.Printf("Server %d 发送 AppendEntries(心跳) to %d, term:%d, commitIndex:%d", rf.me, index, rf.currentTerm.Load(), rf.commitIndex)
					} else {
						//log.Printf("Server %d 发送 AppendEntries to %d, 此次发送log数量: %d", rf.me, index, len(args.Entries))
					}

					ok := rf.sendAppendEntriesRequest(index, &args, &reply)

					if !ok {
						////log.Printf("Server %d 发送 AppendEntries to %d 失败", rf.me, index)
						return

					}
					if reply.Term > rf.currentTerm.Load() { //对于任何reply，如果term比自己大，就要更新自己的term并转为follower
						rf.currentTerm.Store(reply.Term)
						rf.state.Store(Follower)

						rf.refreshElectionTimeout()
						rf.mu.Lock()
						rf.lastReceiveTime = time.Now()
						rf.mu.Unlock()
						return
					}
					if rf.state.Load() != Leader {
						return
					}
					if reply.Success == false {
						rf.mu3.Lock()
						//log.Printf("Server %d 发送 AppendEntries to %d 未被接受, nextIndex: %d", rf.me, index, rf.nextIndex[index])
						rf.nextIndex[index]--
						args = AppendEntriesArgs{
							Term:         args.Term,
							Entries:      logs[rf.nextIndex[index]:],
							PrevLogIndex: rf.nextIndex[index] - 1,
							PrevLogTerm:  logs[rf.nextIndex[index]-1].Term,
							LeaderCommit: args.LeaderCommit,
						}
						//args.Entries = args.Entries[:len(args.Entries)-1]
						//args.PrevLogIndex = args.PrevLogIndex - 1
						//args.PrevLogTerm = logs[rf.nextIndex[index]-1].Term
						//log.Printf("Server %d 准备重发 AppendEntries to %d, nextIndex: %d", rf.me, index, rf.nextIndex[index])
						rf.mu3.Unlock()
					} else {
						rf.mu3.Lock()
						rf.nextIndex[index] = logsLen
						rf.mu3.Unlock()
						mu2.Lock()
						count++
						mu2.Unlock()
						cond.Signal()
						//log.Printf("Server %d 发送 AppendEntries to %d 被接受, nextIndex: %d", rf.me, index, rf.nextIndex[index])
					}
				}

			}(i)
		}

	}
	mu2.Lock()
	for count <= len(rf.peers)/2 {
		cond.Wait()
	}
	mu2.Unlock()
	rf.mu3.Lock()
	for ; rf.commitIndex < logsLen-1; rf.commitIndex++ {
		rf.applyCh <- ApplyMsg{CommandValid: true, Command: rf.log[rf.commitIndex+1].Command, CommandIndex: rf.commitIndex + 1}
		//log.Printf("leader %d apply %d", rf.me, rf.commitIndex+1)
	}
	//log.Printf("leader %d commitIndex更新为 %d", rf.me, rf.commitIndex)
	rf.mu3.Unlock()

}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.state.Store(Follower)
	//log.Printf("server %d is killed", rf.me)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here (2A)
		// Check if a leader election should be started.
		rf.mu.Lock()

		lastReceive := rf.lastReceiveTime
		rf.mu.Unlock()
		if rf.state.Load() != Leader && time.Since(lastReceive) > time.Duration(rf.electionTimeOut.Load())*time.Millisecond {
			go rf.startElection()
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
		////log.Printf("server %d is running, state: %d", rf.me, rf.state.Load())
	}
}

func (rf *Raft) startElection() {
	//log.Printf("Server %d starting election, current term: %d", rf.me, rf.currentTerm.Load())
	rf.currentTerm.Add(1)
	rf.state.Store(Candidate)
	rf.votedFor.Store(int32(rf.me))

	rf.refreshElectionTimeout()

	rf.mu.Lock()
	rf.lastReceiveTime = time.Now()
	rf.mu.Unlock()

	count := 1
	finished := 1
	mu2 := sync.Mutex{}
	cond := sync.NewCond(&mu2)

	for i := range rf.peers {
		if i == rf.me {
			continue
		} else {
			go func(index int) {
				//log.Printf("Server %d sending RequestVote to %d", rf.me, index)
				rf.mu3.Lock()
				args := RequestVoteArgs{Term: rf.currentTerm.Load(), PleaseVoteFor: rf.me, LastLogIndex: rf.commitIndex, LastlogTerm: rf.log[rf.commitIndex].Term}
				rf.mu3.Unlock()
				reply := RequestVoteReply{Voted: false}
				rf.sendRequestVote(index, &args, &reply)
				if reply.Term > rf.currentTerm.Load() { //对于任何reply，如果term比自己大，就要更新自己的term并转为follower
					rf.currentTerm.Store(reply.Term)
					rf.state.Store(Follower)

					rf.refreshElectionTimeout()
					rf.mu.Lock()
					rf.lastReceiveTime = time.Now()
					rf.mu.Unlock()
					return
				}
				mu2.Lock()
				if reply.Voted && reply.Term == rf.currentTerm.Load() {
					count++
				}
				finished++
				cond.Broadcast()
				mu2.Unlock()
			}(i)
		}
	}

	mu2.Lock()
	for count <= len(rf.peers)/2 && finished-count < len(rf.peers)-len(rf.peers)/2 {
		//log.Printf("Server %d waiting for election to finish, count: %d, finished: %d, 获得%d/%d票后赢得选举, 或者获得%d反对票后选举失败", rf.me, count, finished, len(rf.peers)/2+1, len(rf.peers), len(rf.peers)-len(rf.peers)/2)
		cond.Wait()
	}
	if count > len(rf.peers)/2 {
		// Won the election
		//log.Printf("Server %d won the election, count: %d, finished: %d, term: %d", rf.me, count, finished, rf.currentTerm.Load())
		rf.state.Store(Leader)

		rf.mu3.Lock()
		rf.nextIndex = make([]int, len(rf.peers))
		for i := range rf.nextIndex {
			rf.nextIndex[i] = len(rf.log)
		}
		rf.mu3.Unlock()

		go func() {
			for rf.state.Load() == Leader {

				go rf.broadcastAppendEntries()
				time.Sleep(100 * time.Millisecond)
			}
		}()

		go func() {
			rf.mu4.Lock()
			for rf.state.Load() == Leader {
				rf.cond.Wait()
				time.Sleep(5 * time.Millisecond)
				go rf.broadcastAppendEntries()
			}
			rf.mu4.Unlock()
		}()
	}
	mu2.Unlock()
}

func (rf *Raft) refreshElectionTimeout() {
	rf.electionTimeOut.Store(rand.Int31n(1000) + 1000)
}

func (rf *Raft) broadcastHeartBeat() {
	for i := range rf.peers {
		if i == rf.me {
			continue
		} else {
			go func(index int) {
				rf.mu3.Lock()
				//log.Printf("Server %d sending 心跳信号 to %d, term:%d, commitIndex:%d", rf.me, index, rf.currentTerm.Load(), rf.commitIndex)
				args := AppendEntriesArgs{Term: rf.currentTerm.Load(), Entries: nil, LeaderCommit: rf.commitIndex, PrevLogIndex: rf.nextIndex[index] - 1, PrevLogTerm: rf.log[rf.nextIndex[index]-1].Term}
				rf.mu3.Unlock()
				reply := AppendEntriesReply{}
				ok := rf.sendAppendEntriesRequest(index, &args, &reply)
				if !ok {
					return
				}
				if reply.Term > rf.currentTerm.Load() { //对于任何reply，如果term比自己大，就要更新自己的term并转为follower
					rf.currentTerm.Store(reply.Term)
					rf.state.Store(Follower)

					rf.refreshElectionTimeout()
					rf.mu.Lock()
					rf.lastReceiveTime = time.Now()
					rf.mu.Unlock()
					return
				}
				////log.Printf("server %d 受到 %d 的 reply: %v", rf.me, index, reply)
				if reply.Success == false {
					////log.Printf("由于follower含有过时的日志, Server %d 心跳信号 to %d 被拒绝, 准备发送appendRequest", rf.me, index)

					//log.Printf("Server %d 发送 AppendEntries(心跳) to %d 未被接受, nextIndex: %d", rf.me, index, rf.nextIndex[index])
					rf.nextIndex[index]--
					rf.sendAppendRequestToi(index)
				}
			}(i)
		}
	}
}

func (rf *Raft) sendAppendRequestToi(index int) {
	//log.Printf("开始修正")
	rf.mu3.Lock()

	logs := rf.log
	commitIndex := rf.commitIndex
	logsLen := len(rf.log)
	//rf.nextIndex[index]--
	args := AppendEntriesArgs{
		Term:         rf.currentTerm.Load(),
		Entries:      logs[rf.nextIndex[index]:],
		PrevLogIndex: rf.nextIndex[index] - 1,
		PrevLogTerm:  logs[rf.nextIndex[index]-1].Term,
		LeaderCommit: commitIndex,
	}

	rf.mu3.Unlock()
	var reply AppendEntriesReply
	for reply.Success == false && rf.state.Load() == Leader {
		reply = AppendEntriesReply{Success: false}
		ok := rf.sendAppendEntriesRequest(index, &args, &reply)
		//log.Printf("Server %d 发送 AppendEntries to %d, 此次发送log数量: %d", rf.me, index, len(args.Entries))
		if !ok {
			//log.Printf("Server %d 发送 AppendEntries to %d 失败", rf.me, index)
			continue
		}
		if reply.Term > rf.currentTerm.Load() { //对于任何reply，如果term比自己大，就要更新自己的term并转为follower
			rf.currentTerm.Store(reply.Term)
			rf.state.Store(Follower)

			rf.refreshElectionTimeout()
			rf.mu.Lock()
			rf.lastReceiveTime = time.Now()
			rf.mu.Unlock()
			return
		}
		if reply.Success == false {
			rf.mu3.Lock()
			//log.Printf("Server %d 发送 AppendEntries to %d 未被接受, nextIndex: %d", rf.me, index, rf.nextIndex[index])
			rf.nextIndex[index]--
			args = AppendEntriesArgs{
				Entries:      logs[rf.nextIndex[index]:],
				PrevLogIndex: args.PrevLogIndex - 1,
				PrevLogTerm:  logs[rf.nextIndex[index]-1].Term,
			}
			//args.Entries = args.Entries[:len(args.Entries)-1]
			//args.PrevLogIndex = args.PrevLogIndex - 1
			//args.PrevLogTerm = logs[rf.nextIndex[index]-1].Term
			//log.Printf("Server %d 准备重发 AppendEntries to %d, nextIndex: %d", rf.me, index, rf.nextIndex[index])
			rf.mu3.Unlock()
		} else {
			rf.mu3.Lock()
			rf.nextIndex[index] = logsLen
			rf.mu3.Unlock()

			//log.Printf("Server %d 发送 AppendEntries to %d 被接受, nextIndex: %d", rf.me, index, rf.nextIndex[index])
		}
	}
	//log.Printf("end")
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	log.Default()
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state.Store(Follower)
	rf.currentTerm.Store(1)
	rf.votedFor.Store(-1)
	rf.refreshElectionTimeout()
	rf.lastReceiveTime = time.Now()
	rf.log = append(rf.log, LogEntry{Term: 0, Index: 0, Command: nil})
	rf.cond = sync.NewCond(&rf.mu4)
	rf.applyCh = applyCh
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}
