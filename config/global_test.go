package config

import (
	"os"
	"strconv"
	"testing"
)

func TestNewGlobalConfig(t *testing.T) {
	g := NewGlobalConfig()

	if g == nil {
		t.Error("NewGlobalConfig() returned nil")
	}

	if g.Name == "" {
		t.Error("Expected Name to be set")
	}

	if g.Port <= 0 {
		t.Errorf("Expected Port to be positive, got %d", g.Port)
	}

	if g.Host == "" {
		t.Error("Expected Host to be set")
	}

	if g.MaxConn <= 0 {
		t.Errorf("Expected MaxConn to be positive, got %d", g.MaxConn)
	}

	if len(g.Peers) == 0 {
		t.Error("Expected at least 1 peer")
	}
}

func TestGlobalConfigInit(t *testing.T) {
	g := &GlobalConfig{}
	g.Init()

	if g.Name == "" {
		t.Error("Expected Name to be set after Init()")
	}
}

func TestParseFlags(t *testing.T) {
	originalArgs := os.Args
	defer func() { os.Args = originalArgs }()

	os.Args = []string{"test", "-me", "0"}

	g := &GlobalConfig{
		Peers: []string{"localhost:8080", "localhost:8081"},
	}
	g.ParseFlags()

	if g.Me != 0 {
		t.Errorf("Expected Me to be 0, got %d", g.Me)
	}
}

func TestParseFlagsWithEnv(t *testing.T) {
	originalArgs := os.Args
	originalEnv := os.Getenv("RAFT_ME")
	defer func() {
		os.Args = originalArgs
		os.Setenv("RAFT_ME", originalEnv)
	}()

	os.Args = []string{"test"}
	os.Unsetenv("RAFT_ME")
	os.Setenv("RAFT_ME", "1")

	g := &GlobalConfig{
		Peers: []string{"localhost:8080", "localhost:8081"},
		Me:    -1,
	}
	g.ParseFlags()

	if g.Me != 1 {
		t.Errorf("Expected Me to be 1 from env, got %d", g.Me)
	}
}

func TestParseFlagsPriority(t *testing.T) {
	originalArgs := os.Args
	originalEnv := os.Getenv("RAFT_ME")
	defer func() {
		os.Args = originalArgs
		os.Setenv("RAFT_ME", originalEnv)
	}()

	os.Args = []string{"test", "-me", "0"}
	os.Setenv("RAFT_ME", "1")

	g := &GlobalConfig{
		Peers: []string{"localhost:8080", "localhost:8081"},
	}
	g.ParseFlags()

	if g.Me != 0 {
		t.Errorf("Expected Me to be 0 (flag priority), got %d", g.Me)
	}
}

func TestDefaultConfigValues(t *testing.T) {
	g := NewGlobalConfig()

	expectedDefaults := []struct {
		name  string
		value interface{}
	}{
		{"Version", "1.0.0"},
		{"MaxPackageSize", uint32(16 << 20)},
		{"WorkerPoolSize", uint32(10)},
		{"MaxWorkerTaskLen", uint32(10000)},
		{"MaxMsgChanLen", uint32(100)},
		{"MaxMemTableP", 0.5},
		{"MaxMemTableLevel", 32},
		{"MaxMemTableSize", 1024},
		{"RaftSnapshotThreshold", 1000},
		{"RaftSnapshotKeepEntries", 100},
	}

	for _, tt := range expectedDefaults {
		t.Run(tt.name, func(t *testing.T) {
			switch tt.name {
			case "Version":
				if g.Version != tt.value.(string) {
					t.Errorf("Expected Version to be '%s', got '%s'", tt.value, g.Version)
				}
			case "MaxPackageSize":
				if g.MaxPackageSize != tt.value.(uint32) {
					t.Errorf("Expected MaxPackageSize to be %d, got %d", tt.value, g.MaxPackageSize)
				}
			case "WorkerPoolSize":
				if g.WorkerPoolSize != tt.value.(uint32) {
					t.Errorf("Expected WorkerPoolSize to be %d, got %d", tt.value, g.WorkerPoolSize)
				}
			case "MaxWorkerTaskLen":
				if g.MaxWorkerTaskLen != tt.value.(uint32) {
					t.Errorf("Expected MaxWorkerTaskLen to be %d, got %d", tt.value, g.MaxWorkerTaskLen)
				}
			case "MaxMsgChanLen":
				if g.MaxMsgChanLen != tt.value.(uint32) {
					t.Errorf("Expected MaxMsgChanLen to be %d, got %d", tt.value, g.MaxMsgChanLen)
				}
			case "MaxMemTableP":
				if g.MaxMemTableP != tt.value.(float64) {
					t.Errorf("Expected MaxMemTableP to be %f, got %f", tt.value, g.MaxMemTableP)
				}
			case "MaxMemTableLevel":
				if g.MaxMemTableLevel != tt.value.(int) {
					t.Errorf("Expected MaxMemTableLevel to be %d, got %d", tt.value, g.MaxMemTableLevel)
				}
			case "MaxMemTableSize":
				if g.MaxMemTableSize != tt.value.(int) {
					t.Errorf("Expected MaxMemTableSize to be %d, got %d", tt.value, g.MaxMemTableSize)
				}
			case "RaftSnapshotThreshold":
				if g.RaftSnapshotThreshold != tt.value.(int) {
					t.Errorf("Expected RaftSnapshotThreshold to be %d, got %d", tt.value, g.RaftSnapshotThreshold)
				}
			case "RaftSnapshotKeepEntries":
				if g.RaftSnapshotKeepEntries != tt.value.(int) {
					t.Errorf("Expected RaftSnapshotKeepEntries to be %d, got %d", tt.value, g.RaftSnapshotKeepEntries)
				}
			}
		})
	}
}

func TestConfigInitWithFile(t *testing.T) {
	originalArgs := os.Args
	defer func() { os.Args = originalArgs }()

	os.Args = []string{"test"}

	g := NewGlobalConfig()

	if g == nil {
		t.Fatal("NewGlobalConfig() returned nil")
	}

	t.Logf("Loaded config: Name=%s, Host=%s, Port=%d, MaxConn=%d", g.Name, g.Host, g.Port, g.MaxConn)
}

func TestPeerValidation(t *testing.T) {
	testCases := []struct {
		name    string
		mode    string
		peers   []string
		me      int
		wantErr bool
	}{
		{"valid single node", ModeRaft, []string{"localhost:8080"}, 0, false},
		{"valid multi node", ModeRaft, []string{"localhost:8080", "localhost:8081", "localhost:8082"}, 1, false},
		{"invalid out of range", ModeRaft, []string{"localhost:8080"}, 1, true},
		// standalone 不启动 Raft，越界的 Me 不再触发校验
		{"standalone skips validation", ModeStandalone, []string{"localhost:8080"}, 1, false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			originalArgs := os.Args
			defer func() { os.Args = originalArgs }()

			os.Args = []string{"test", "-me", strconv.Itoa(tc.me)}

			g := &GlobalConfig{
				Mode:  tc.mode,
				Peers: tc.peers,
			}

			defer func() {
				if r := recover(); r != nil {
					if !tc.wantErr {
						t.Errorf("Expected no panic, but got: %v", r)
					}
				} else {
					if tc.wantErr {
						t.Error("Expected panic for invalid configuration")
					}
				}
			}()

			g.ParseFlags()
		})
	}
}

func TestConfigPathSearch(t *testing.T) {
	g := &GlobalConfig{}
	g.Init()

	if g.Name == "" {
		t.Error("Expected Name to be set after Init()")
	}
}
