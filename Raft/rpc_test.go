package Raft

import (
	"net"
	"net/rpc"
	"testing"
	"time"
)

func TestRequestVoteRPC(t *testing.T) {
	peers := []string{"localhost:8000", "localhost:8001"}
	r1 := NewRaft(peers, 0)
	r2 := NewRaft(peers, 1)

	server1 := rpc.NewServer()
	rpc1 := NewRaftRPC(r1)
	rpc1.RegisterRPC(server1)

	server2 := rpc.NewServer()
	rpc2 := NewRaftRPC(r2)
	rpc2.RegisterRPC(server2)

	go func() {
		ln, err := net.Listen("tcp", "localhost:8000")
		if err != nil {
			t.Errorf("Failed to listen: %v", err)
			return
		}
		defer ln.Close()
		server1.Accept(ln)
	}()

	go func() {
		ln, err := net.Listen("tcp", "localhost:8001")
		if err != nil {
			t.Errorf("Failed to listen: %v", err)
			return
		}
		defer ln.Close()
		server2.Accept(ln)
	}()

	time.Sleep(100 * time.Millisecond)

	args := &RequestVoteArgs{
		Term:         5, // higher term to win the vote
		CandidateID:  0,
		LastLogIndex: -1,
		LastLogTerm:  0,
	}

	reply, err := r1.SendRequestVote("localhost:8001", args)
	if err != nil {
		t.Fatalf("SendRequestVote failed: %v", err)
	}

	if !reply.VoteGranted {
		t.Error("Expected vote to be granted")
	}

	if reply.Term != 5 {
		t.Errorf("Expected term 5, got %d", reply.Term)
	}
}

func TestAppendEntriesRPC(t *testing.T) {
	peers := []string{"localhost:8002", "localhost:8003"}
	r1 := NewRaft(peers, 0)
	r2 := NewRaft(peers, 1)

	server1 := rpc.NewServer()
	rpc1 := NewRaftRPC(r1)
	rpc1.RegisterRPC(server1)

	server2 := rpc.NewServer()
	rpc2 := NewRaftRPC(r2)
	rpc2.RegisterRPC(server2)

	go func() {
		ln, err := net.Listen("tcp", "localhost:8002")
		if err != nil {
			t.Errorf("Failed to listen: %v", err)
			return
		}
		defer ln.Close()
		server1.Accept(ln)
	}()

	go func() {
		ln, err := net.Listen("tcp", "localhost:8003")
		if err != nil {
			t.Errorf("Failed to listen: %v", err)
			return
		}
		defer ln.Close()
		server2.Accept(ln)
	}()

	time.Sleep(100 * time.Millisecond)

	args := &AppendEntriesArgs{
		Term:         1,
		LeaderID:     0,
		PrevLogIndex: -1,
		PrevLogTerm:  0,
		Entries: []LogEntry{
			{Index: 0, Term: 1, Command: []byte("test command")},
		},
		LeaderCommit: -1,
	}

	reply, err := r1.SendAppendEntries("localhost:8003", args)
	if err != nil {
		t.Fatalf("SendAppendEntries failed: %v", err)
	}

	if !reply.Success {
		t.Error("Expected append to be successful")
	}

	if reply.Term != 1 {
		t.Errorf("Expected term 1, got %d", reply.Term)
	}

	log := r2.GetLog()
	if len(log) != 1 {
		t.Errorf("Expected 1 log entry, got %d", len(log))
	}

	if string(log[0].Command) != "test command" {
		t.Errorf("Expected command 'test command', got '%s'", string(log[0].Command))
	}
}
