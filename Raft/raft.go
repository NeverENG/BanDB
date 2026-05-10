package Raft

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/NeverENG/BanDB/config"
)

const (
	MinElectionTimeout = 150 * time.Millisecond
	MaxElectionTimeout = 300 * time.Millisecond
	HeartbeatInterval  = 50 * time.Millisecond
)

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

type LogEntry struct {
	Index      int
	Term       int
	Command    []byte
	IsSnapshot bool
}

type Raft struct {
	peers    []string
	me       int
	state    State
	votedFor int
	Term     int
	mu       sync.Mutex

	electionTimeout time.Duration
	timer           *time.Timer
	heartbeatTicker *time.Ticker

	commitIndex       int
	lastApplied       int
	lastSnapshotIndex int

	nextIndex  []int
	matchIndex []int
	log        []LogEntry

	electionCh  chan bool
	heartbeatCh chan bool
	ApplyCh     chan LogEntry

	LastIncludedIndex int64
	LastIncludedTerm  int64

	wal     *RaftWAL
	addrMap map[int]string

	commitCond *sync.Cond
}

func NewRaft(peers []string, me int) *Raft {
	addrMap := make(map[int]string)
	for i, addr := range peers {
		addrMap[i] = addr
	}

	r := &Raft{
		peers:           peers,
		me:              me,
		state:           Follower,
		votedFor:        -1,
		Term:            0,
		electionTimeout: MinElectionTimeout + time.Duration(rand.Int63n(int64(MaxElectionTimeout-MinElectionTimeout))),
		commitIndex:     -1,
		lastApplied:     -1,

		nextIndex:   make([]int, len(peers)),
		matchIndex:  make([]int, len(peers)),
		log:         make([]LogEntry, 0),
		electionCh:  make(chan bool),
		heartbeatCh: make(chan bool),
		ApplyCh:     make(chan LogEntry, 100),
		addrMap:     addrMap,
	}

	wal, _ := NewRaftWAL("raft_data")

	r.wal = wal

	// 从磁盘加载持久化状态（currentTerm, votedFor, log, snapshot metadata）
	if err := r.readPersist(); err != nil {
		fmt.Printf("[RAFT WARN] Failed to load persisted state: %v\n", err)
	}

	// 如果有快照，通知 FSM
	if r.LastIncludedIndex > 0 && r.ApplyCh != nil {
		snapshotData, _, _, err := wal.LoadLatestSnapshot()
		if err == nil && snapshotData != nil {
			select {
			case r.ApplyCh <- LogEntry{
				Index:      int(r.LastIncludedIndex),
				Term:       int(r.LastIncludedTerm),
				Command:    snapshotData,
				IsSnapshot: true,
			}:
			default:
				fmt.Println("[WARN] ApplyCh is full during initialization, snapshot skipped")
			}
		}
	}

	r.commitCond = sync.NewCond(&r.mu)

	go r.electionLoop()

	return r
}

// persistLocked 持久化 Raft 状态（必须在持有锁的情况下调用）
func (r *Raft) persistLocked() {
	data := PersistData{
		CurrentTerm:       int64(r.Term),
		VotedFor:          int64(r.votedFor),
		Log:               r.log,
		LastIncludedIndex: r.LastIncludedIndex,
		LastIncludedTerm:  r.LastIncludedTerm,
	}

	if err := r.wal.SavePersist(data); err != nil {
		fmt.Printf("[RAFT ERROR] Failed to persist state: %v\n", err)
	}
}

// readPersist 从磁盘加载 Raft 状态
func (r *Raft) readPersist() error {
	data, err := r.wal.LoadPersist()
	if err != nil {
		return err
	}

	r.Term = int(data.CurrentTerm)
	r.votedFor = int(data.VotedFor)
	r.log = data.Log
	r.LastIncludedIndex = data.LastIncludedIndex
	r.LastIncludedTerm = data.LastIncludedTerm

	if r.LastIncludedIndex > 0 {
		r.commitIndex = int(r.LastIncludedIndex)
		r.lastApplied = int(r.LastIncludedIndex)
		r.lastSnapshotIndex = int(r.LastIncludedIndex)
	}

	return nil
}
func (r *Raft) Start() {
	if r.state == Leader {
		r.startHeartbeatLoop()
	}
}

func (r *Raft) electionLoop() {
	for {
		timeout := MinElectionTimeout + time.Duration(rand.Int63n(int64(MaxElectionTimeout-MinElectionTimeout)))
		r.timer = time.NewTimer(timeout)

		select {
		case <-r.timer.C:
			r.startElection()
		case <-r.heartbeatCh:
			r.timer.Reset(timeout)
		case <-r.electionCh:
			r.timer.Reset(timeout)
		}
	}
}

func (r *Raft) startElection() {
	r.mu.Lock()

	if r.state == Leader {
		r.mu.Unlock()
		return
	}

	fmt.Printf("[RAFT] Starting election, current state=%v, Term=%d\n", r.state, r.Term)

	r.state = Candidate
	r.Term++
	r.votedFor = r.me
	r.persistLocked() // 持久化 Term 和 votedFor

	lastLogIndex := -1
	lastLogTerm := 0
	if len(r.log) > 0 {
		lastLogIndex = len(r.log) - 1
		lastLogTerm = r.log[lastLogIndex].Term
	}

	args := &RequestVoteArgs{
		Term:         r.Term,
		CandidateID:  r.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	peerCount := len(r.peers) - 1
	votes := 1
	voteCh := make(chan bool, peerCount+1)
	voteCh <- true

	for i := range r.peers {
		if i == r.me {
			continue
		}

		go func(peerID int) {
			reply, err := r.SendRequestVote(r.addrMap[peerID], args)
			if err != nil {
				voteCh <- false
				return
			}

			r.mu.Lock()
			defer r.mu.Unlock()

			if reply.Term > r.Term {
				r.Term = reply.Term
				r.state = Follower
				r.votedFor = -1
				voteCh <- false
				return
			}

			if reply.Term == r.Term && reply.VoteGranted {
				voteCh <- true
			} else {
				voteCh <- false
			}
		}(i)
	}

	r.mu.Unlock()

	// 自己投自己一票

	// 单节点模式：自己一票就超过半数，直接成为 Leader
	if votes > len(r.peers)/2 {
		r.mu.Lock()
		if r.state == Candidate {
			r.becomeLeader()
		}
		r.mu.Unlock()
		return
	}

	// 等待投票结果或超时
	timeout := time.After(500 * time.Millisecond)
	for j := 0; j < peerCount; j++ {
		select {
		case voteGranted := <-voteCh:
			if voteGranted {
				votes++
				// 获得多数票，成为 Leader
				if votes > len(r.peers)/2 {
					r.mu.Lock()
					if r.state == Candidate {
						r.becomeLeader()
					}
					r.mu.Unlock()
					return
				}
			}
		case <-timeout:
			// 选举超时，重置为 Follower
			r.mu.Lock()
			if r.state == Candidate {
				r.state = Follower
				r.votedFor = -1
			}
			r.mu.Unlock()
			return
		}
	}
}

func (r *Raft) becomeLeader() {
	fmt.Printf("[RAFT] Becoming Leader, Term=%d\n", r.Term)
	r.state = Leader

	for i := range r.peers {
		r.nextIndex[i] = len(r.log)
		r.matchIndex[i] = -1
	}

	fmt.Printf("[RAFT] Started heartbeat loop\n")
	r.startHeartbeatLoop()
}

func (r *Raft) startHeartbeatLoop() {
	if r.heartbeatTicker != nil {
		r.heartbeatTicker.Stop()
	}

	r.heartbeatTicker = time.NewTicker(HeartbeatInterval)
	go func() {
		for r.state == Leader {
			<-r.heartbeatTicker.C
			r.SendHeartBeat()
		}
	}()
}

func (r *Raft) SendHeartBeat() {
	r.mu.Lock()
	if r.state != Leader {
		r.mu.Unlock()
		return
	}

	for i := range r.peers {
		if i == r.me {
			continue
		}

		prevLogIndex := r.nextIndex[i] - 1
		prevLogTerm := 0
		if prevLogIndex >= 0 && prevLogIndex < len(r.log) {
			prevLogTerm = r.log[prevLogIndex].Term
		}

		args := &AppendEntriesArgs{
			Term:         r.Term,
			LeaderID:     r.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      []LogEntry{},
			LeaderCommit: r.commitIndex,
		}

		r.mu.Unlock()

		go func(peerID int, args *AppendEntriesArgs) {
			reply, err := r.SendAppendEntries(r.addrMap[peerID], args)
			if err != nil {
				r.mu.Lock()
				if r.state == Leader {
					r.nextIndex[peerID]--
				}
				r.mu.Unlock()
				return
			}

			r.mu.Lock()
			defer r.mu.Unlock()

			if reply.Term > r.Term {
				r.Term = reply.Term
				r.state = Follower
				r.votedFor = -1
				r.heartbeatTicker.Stop()
				return
			}

			if reply.Success {
				r.nextIndex[peerID] = len(r.log)
				r.matchIndex[peerID] = len(r.log) - 1
				r.updateCommitIndex()
			} else {
				r.nextIndex[peerID]--
			}
		}(i, args)

		r.mu.Lock()
	}
	r.mu.Unlock()
}

func (r *Raft) updateCommitIndex() {
	if r.state != Leader {
		return
	}

	for n := len(r.log) - 1; n > r.commitIndex; n-- {
		count := 1
		for i := range r.peers {
			if i != r.me && r.matchIndex[i] >= n {
				count++
			}
		}
		if count > len(r.peers)/2 && r.log[n].Term == r.Term {
			r.commitIndex = n
			r.applyCommittedLogs()
			r.commitCond.Broadcast()
			break
		}
	}
}

func (r *Raft) applyCommittedLogs() {
	for r.lastApplied < r.commitIndex {
		r.lastApplied++
		if r.ApplyCh != nil {
			r.ApplyCh <- r.log[r.lastApplied]
		}
	}

	// 检查是否需要触发快照
	r.checkSnapshotTrigger()
}

// checkSnapshotTrigger 检查是否应该触发快照
func (r *Raft) checkSnapshotTrigger() {
	if r.state != Leader {
		return
	}

	// 如果日志长度超过阈值，触发快照
	logLength := len(r.log)
	threshold := 1000  // 默认阈值
	keepEntries := 100 // 保留的条目数

	// 从配置中读取（如果可用）
	if config.G.RaftSnapshotThreshold > 0 {
		threshold = config.G.RaftSnapshotThreshold
	}
	if config.G.RaftSnapshotKeepEntries > 0 {
		keepEntries = config.G.RaftSnapshotKeepEntries
	}

	if logLength > threshold {
		// 计算快照索引：保留最新的 keepEntries 条日志
		snapshotIndex := r.commitIndex - keepEntries
		if snapshotIndex > r.lastSnapshotIndex {
			fmt.Printf("[RAFT] Triggering snapshot: log length=%d, threshold=%d, snapshot index=%d\n",
				logLength, threshold, snapshotIndex)

			// 这里需要上层应用提供快照数据
			// 实际使用时，应该通过回调或通道请求 FSM 生成快照
			// TODO: 实现快照生成逻辑
		}
	}
}

func (r *Raft) AppendEntry(command []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state != Leader {
		fmt.Printf("[RAFT] AppendEntry failed: not leader, state=%v\n", r.state)
		return -1, fmt.Errorf("not leader")
	}

	entry := LogEntry{
		Index:   len(r.log),
		Term:    r.Term,
		Command: command,
	}
	r.log = append(r.log, entry)
	r.persistLocked() // 持久化日志条目

	fmt.Printf("[RAFT] Appended entry: Index=%d, Term=%d\n", entry.Index, entry.Term)

	// 单节点模式：立即提交
	if len(r.peers) == 1 {
		r.commitIndex = entry.Index
		fmt.Printf("[RAFT] Single node mode, committed index: %d\n", r.commitIndex)
		r.applyCommittedLogs()
		r.commitCond.Broadcast()
	} else {
		r.replicateLog()
	}

	return entry.Index, nil
}

func (r *Raft) replicateLog() {
	if r.state != Leader {
		return
	}

	for i := range r.peers {
		if i == r.me {
			continue
		}

		prevLogIndex := r.nextIndex[i] - 1
		prevLogTerm := 0
		if prevLogIndex >= 0 && prevLogIndex < len(r.log) {
			prevLogTerm = r.log[prevLogIndex].Term
		}

		var entries []LogEntry
		if r.nextIndex[i] < len(r.log) {
			entries = r.log[r.nextIndex[i]:]
		}

		args := &AppendEntriesArgs{
			Term:         r.Term,
			LeaderID:     r.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: r.commitIndex,
		}

		go func(peerID int, args *AppendEntriesArgs) {
			reply, err := r.SendAppendEntries(r.addrMap[peerID], args)
			if err != nil {
				r.mu.Lock()
				if r.state == Leader {
					r.nextIndex[peerID]--
				}
				r.mu.Unlock()
				return
			}

			r.mu.Lock()
			defer r.mu.Unlock()

			if reply.Term > r.Term {
				r.Term = reply.Term
				r.state = Follower
				r.votedFor = -1
				r.heartbeatTicker.Stop()
				return
			}

			if reply.Success {
				r.nextIndex[peerID] = len(r.log)
				r.matchIndex[peerID] = len(r.log) - 1
				r.updateCommitIndex()
			} else {
				r.nextIndex[peerID]--
			}
		}(i, args)
	}
}

func (r *Raft) WaitCommitIndex(index int) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for r.commitIndex < index {
		r.commitCond.Wait()
	}
}

func (r *Raft) GetState() (State, int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.state, r.Term
}

func (r *Raft) GetLog() []LogEntry {
	r.mu.Lock()
	defer r.mu.Unlock()
	logCopy := make([]LogEntry, len(r.log))
	copy(logCopy, r.log)
	return logCopy
}

func (r *Raft) GetApplyCh() chan LogEntry {
	return r.ApplyCh
}

// GetCommitIndex 获取当前提交索引
func (r *Raft) GetCommitIndex() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.commitIndex
}

func (r *Raft) TakeSnapshot(index int, data []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if index <= r.lastSnapshotIndex {
		return fmt.Errorf("snapshot index %d is not greater than last snapshot index %d", index, r.lastSnapshotIndex)
	}

	if index > r.commitIndex {
		return fmt.Errorf("cannot snapshot uncommitted index %d, commitIndex is %d", index, r.commitIndex)
	}

	// 获取快照包含的最后一条日志的 term
	var term int
	if index == int(r.LastIncludedIndex) {
		term = int(r.LastIncludedTerm)
	} else {
		// 将绝对索引转换为 log 数组的相对索引
		logIndex := index - int(r.LastIncludedIndex) - 1
		if logIndex < 0 || logIndex >= len(r.log) {
			return fmt.Errorf("invalid snapshot index %d, LastIncludedIndex=%d, log length=%d",
				index, r.LastIncludedIndex, len(r.log))
		}
		term = r.log[logIndex].Term
	}

	// 1. 先保存快照到磁盘
	if err := r.wal.SaveSnapshot(data, int64(index), int64(term)); err != nil {
		return fmt.Errorf("failed to save snapshot: %w", err)
	}

	// 2. 删除旧快照
	r.wal.DeleteOldSnapshots(int64(index))

	// 3. 截断 WAL 日志（删除快照包含的日志）
	if err := r.wal.TruncateLogs(int64(index)); err != nil {
		return fmt.Errorf("failed to truncate logs: %w", err)
	}

	// 4. 清理内存中的日志并重新编号
	newLogStart := index - int(r.LastIncludedIndex)
	if newLogStart > 0 && newLogStart <= len(r.log) {
		r.log = r.log[newLogStart:]
		for i := range r.log {
			r.log[i].Index = index + 1 + i
		}
	} else {
		r.log = []LogEntry{}
	}

	// 5. 更新元数据
	r.lastSnapshotIndex = index
	r.LastIncludedIndex = int64(index)
	r.LastIncludedTerm = int64(term)

	fmt.Printf("[RAFT] Snapshot created: Index=%d, Term=%d\n", index, term)

	// 6. 通知 FSM 应用快照
	if r.ApplyCh != nil {
		snapshotEntry := LogEntry{
			Index:      index,
			Term:       term,
			Command:    data,
			IsSnapshot: true,
		}
		select {
		case r.ApplyCh <- snapshotEntry:
			fmt.Printf("[RAFT] Snapshot notification sent to FSM: Index=%d\n", index)
		default:
			fmt.Println("[WARN] ApplyCh is full, snapshot notification skipped")
		}
	}

	// 7. 持久化状态
	r.persistLocked()

	return nil
}
