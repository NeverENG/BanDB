package Raft

import (
	"encoding/binary"
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

	lastLogIndex := int(r.LastIncludedIndex)
	lastLogTerm := int(r.LastIncludedTerm)
	if len(r.log) > 0 {
		lastLogIndex = r.log[len(r.log)-1].Index
		lastLogTerm = r.log[len(r.log)-1].Term
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

	// 计算下一个日志的绝对索引（考虑快照偏移）
	nextLogIndex := int(r.LastIncludedIndex) + 1
	if len(r.log) > 0 {
		nextLogIndex = r.log[len(r.log)-1].Index + 1
	}

	for i := range r.peers {
		r.nextIndex[i] = nextLogIndex
		r.matchIndex[i] = int(r.LastIncludedIndex)
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

		// 如果 follower 落后太多（prevLogIndex 在快照范围内），发送 InstallSnapshot
		if prevLogIndex < int(r.LastIncludedIndex) && r.LastIncludedIndex > 0 {
			snapshotData, _, _, err := r.wal.LoadLatestSnapshot()
			if err == nil && snapshotData != nil {
				snapArgs := &InstallSnapshotArgs{
					Term:              r.Term,
					LeaderID:          r.me,
					Data:              snapshotData,
					LastIncludedIndex: r.LastIncludedIndex,
					LastIncludedTerm:  r.LastIncludedTerm,
				}
				r.mu.Unlock()
				go func(peerID int, snapArgs *InstallSnapshotArgs) {
					reply, err := r.SendInstallSnapshot(r.addrMap[peerID], snapArgs)
					if err != nil {
						return
					}
					r.mu.Lock()
					defer r.mu.Unlock()
					if reply.Success {
						r.nextIndex[peerID] = int(r.LastIncludedIndex) + 1
						r.matchIndex[peerID] = int(r.LastIncludedIndex)
					} else if reply.Term > r.Term {
						r.Term = reply.Term
						r.state = Follower
						r.votedFor = -1
						r.heartbeatTicker.Stop()
					}
				}(i, snapArgs)
				r.mu.Lock()
				continue
			}
		}

		prevLogTerm := r.getTermAt(prevLogIndex)

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
				r.nextIndex[peerID] = r.getLastLogIndex() + 1
				r.matchIndex[peerID] = r.getLastLogIndex()
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

	// 从后往前遍历日志条目，找到可以提交的
	for i := len(r.log) - 1; i >= 0; i-- {
		n := r.log[i].Index
		if n <= r.commitIndex {
			continue
		}
		if r.log[i].Term != r.Term {
			continue
		}

		count := 1
		for j := range r.peers {
			if j != r.me && r.matchIndex[j] >= n {
				count++
			}
		}
		if count > len(r.peers)/2 {
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
		// 将绝对索引转换为相对数组索引
		relativeIndex := r.lastApplied - int(r.LastIncludedIndex) - 1
		if relativeIndex >= 0 && relativeIndex < len(r.log) {
			if r.ApplyCh != nil {
				r.ApplyCh <- r.log[relativeIndex]
			}
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

	logLength := len(r.log)
	threshold := config.G.RaftSnapshotThreshold
	if threshold <= 0 {
		threshold = 10000
	}
	keepEntries := config.G.RaftSnapshotKeepEntries
	if keepEntries <= 0 {
		keepEntries = 100
	}

	if logLength > threshold {
		snapshotIndex := r.commitIndex - keepEntries
		if snapshotIndex > r.lastSnapshotIndex {
			fmt.Printf("[RAFT] Auto-triggering snapshot at index %d (log length=%d, threshold=%d)\n",
				snapshotIndex, logLength, threshold)
			// 异步调用避免持锁死锁（checkSnapshotTrigger 在持锁上下文中被调用）
			go r.TakeSnapshot(snapshotIndex)
		}
	}
}

// getTermAt 获取指定绝对索引处的日志 term（考虑快照偏移）
func (r *Raft) getTermAt(absIndex int) int {
	if absIndex < 0 {
		return 0
	}
	if absIndex == int(r.LastIncludedIndex) && r.LastIncludedIndex > 0 {
		return int(r.LastIncludedTerm)
	}
	relativeIndex := absIndex - int(r.LastIncludedIndex) - 1
	if relativeIndex >= 0 && relativeIndex < len(r.log) {
		return r.log[relativeIndex].Term
	}
	return 0
}

// getLastLogIndex 获取最后一条日志的绝对索引
func (r *Raft) getLastLogIndex() int {
	if len(r.log) > 0 {
		return r.log[len(r.log)-1].Index
	}
	return int(r.LastIncludedIndex)
}

func (r *Raft) AppendEntry(command []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state != Leader {
		fmt.Printf("[RAFT] AppendEntry failed: not leader, state=%v\n", r.state)
		return -1, fmt.Errorf("not leader")
	}

	// 计算绝对索引（考虑快照偏移）
	lastLogIndex := int(r.LastIncludedIndex)
	if len(r.log) > 0 {
		lastLogIndex = r.log[len(r.log)-1].Index
	}

	entry := LogEntry{
		Index:   lastLogIndex + 1,
		Term:    r.Term,
		Command: command,
	}
	r.log = append(r.log, entry)
	r.persistLocked()

	// 单节点模式：立即提交
	if len(r.peers) == 1 {
		r.commitIndex = entry.Index
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

		// 如果 follower 落后太多（prevLogIndex 在快照范围内），发送 InstallSnapshot
		if prevLogIndex < int(r.LastIncludedIndex) && r.LastIncludedIndex > 0 {
			snapshotData, _, _, err := r.wal.LoadLatestSnapshot()
			if err == nil && snapshotData != nil {
				snapArgs := &InstallSnapshotArgs{
					Term:              r.Term,
					LeaderID:          r.me,
					Data:              snapshotData,
					LastIncludedIndex: r.LastIncludedIndex,
					LastIncludedTerm:  r.LastIncludedTerm,
				}
				go func(peerID int, snapArgs *InstallSnapshotArgs) {
					reply, err := r.SendInstallSnapshot(r.addrMap[peerID], snapArgs)
					if err != nil {
						return
					}
					r.mu.Lock()
					defer r.mu.Unlock()
					if reply.Success {
						r.nextIndex[peerID] = int(r.LastIncludedIndex) + 1
						r.matchIndex[peerID] = int(r.LastIncludedIndex)
					} else if reply.Term > r.Term {
						r.Term = reply.Term
						r.state = Follower
						r.votedFor = -1
						r.heartbeatTicker.Stop()
					}
				}(i, snapArgs)
			}
			continue
		}

		prevLogTerm := r.getTermAt(prevLogIndex)

		// 将绝对索引转换为相对数组索引来切片日志
		var entries []LogEntry
		relativeStart := r.nextIndex[i] - int(r.LastIncludedIndex) - 1
		if relativeStart < len(r.log) {
			entries = r.log[relativeStart:]
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
				r.nextIndex[peerID] = r.getLastLogIndex() + 1
				r.matchIndex[peerID] = r.getLastLogIndex()
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

func (r *Raft) TakeSnapshot(index int) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if index <= r.lastSnapshotIndex {
		return fmt.Errorf("snapshot index %d is not greater than last snapshot index %d", index, r.lastSnapshotIndex)
	}

	if index > r.commitIndex {
		return fmt.Errorf("cannot snapshot uncommitted index %d, commitIndex is %d", index, r.commitIndex)
	}

	// 收集需要放入快照的日志条目（绝对索引 <= index）
	var snapshotEntries []LogEntry
	relativeEnd := index + 1 - int(r.LastIncludedIndex)
	for i := 0; i < relativeEnd && i < len(r.log); i++ {
		snapshotEntries = append(snapshotEntries, r.log[i])
	}

	// 获取最后一条的 term
	var term int
	if len(snapshotEntries) > 0 {
		term = snapshotEntries[len(snapshotEntries)-1].Term
	} else if index == int(r.LastIncludedIndex) {
		term = int(r.LastIncludedTerm)
	}

	// 序列化日志条目为快照数据
	data := SerializeLogEntries(snapshotEntries)

	// 1. 先保存快照到磁盘
	if err := r.wal.SaveSnapshot(data, int64(index), int64(term)); err != nil {
		return fmt.Errorf("failed to save snapshot: %w", err)
	}

	// 2. 删除旧快照
	r.wal.DeleteOldSnapshots(int64(index))

	// 3. 截断 WAL 日志
	if err := r.wal.TruncateLogs(int64(index)); err != nil {
		return fmt.Errorf("failed to truncate logs: %w", err)
	}

	// 4. 清理内存中的日志并重新编号
	newLogStart := index + 1 - int(r.LastIncludedIndex)
	if newLogStart >= 0 && newLogStart <= len(r.log) {
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

	// 6. 通知 FSM 异步重放快照（日志条目序列化数据）
	if r.ApplyCh != nil {
		snapshotEntry := LogEntry{
			Index:      index,
			Term:       term,
			Command:    data,
			IsSnapshot: true,
		}
		select {
		case r.ApplyCh <- snapshotEntry:
			fmt.Printf("[RAFT] Snapshot replay sent to FSM: Index=%d, entries=%d\n", index, len(snapshotEntries))
		default:
			fmt.Println("[WARN] ApplyCh is full, snapshot replay skipped")
		}
	}

	// 7. 持久化状态
	r.persistLocked()

	return nil
}

// SerializeLogEntries 序列化日志条目为字节流（快照数据格式）
func SerializeLogEntries(entries []LogEntry) []byte {
	if len(entries) == 0 {
		return nil
	}

	size := 4 // entry count
	for _, e := range entries {
		size += 8 + 8 + 8 + len(e.Command) // Index(8) + Term(8) + CmdLen(8) + Command
	}

	buf := make([]byte, size)
	offset := 0
	binary.BigEndian.PutUint32(buf[offset:], uint32(len(entries)))
	offset += 4
	for _, e := range entries {
		binary.BigEndian.PutUint64(buf[offset:], uint64(e.Index))
		offset += 8
		binary.BigEndian.PutUint64(buf[offset:], uint64(e.Term))
		offset += 8
		binary.BigEndian.PutUint64(buf[offset:], uint64(len(e.Command)))
		offset += 8
		copy(buf[offset:], e.Command)
		offset += len(e.Command)
	}
	return buf
}

// DeserializeLogEntries 反序列化日志条目
func DeserializeLogEntries(data []byte) []LogEntry {
	if len(data) < 4 {
		return nil
	}

	offset := 0
	count := binary.BigEndian.Uint32(data[offset:])
	offset += 4

	entries := make([]LogEntry, 0, count)
	for i := uint32(0); i < count; i++ {
		if offset+24 > len(data) {
			break
		}
		index := int(binary.BigEndian.Uint64(data[offset:]))
		offset += 8
		term := int(binary.BigEndian.Uint64(data[offset:]))
		offset += 8
		cmdLen := int(binary.BigEndian.Uint64(data[offset:]))
		offset += 8

		if offset+cmdLen > len(data) {
			break
		}
		cmd := make([]byte, cmdLen)
		copy(cmd, data[offset:offset+cmdLen])
		offset += cmdLen

		entries = append(entries, LogEntry{
			Index:   index,
			Term:    term,
			Command: cmd,
		})
	}
	return entries
}
