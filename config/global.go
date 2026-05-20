package config

import (
	"encoding/json"
	"flag"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/NeverENG/BanDB/network/banIface"
)

type GlobalConfig struct {
	Name    string
	Port    int
	Host    string
	Version string

	WALPath           string
	SSTablePath       string
	MaxMemTableSize   int
	MaxCompactionSize int

	TcpServer      banIface.IServer
	MaxConn        int
	MaxPackageSize uint32

	WorkerPoolSize   uint32
	MaxWorkerTaskLen uint32
	MaxMsgChanLen    uint32

	MaxMemTableP     float64
	MaxMemTableLevel int

	// Raft 集群配置
	Peers []string // 集群中所有节点的地址
	Me    int      // 当前节点在 Peers 中的索引（0-based）
	// Raft 快照配置
	RaftSnapshotThreshold   int // 触发快照的日志数量阈值
	RaftSnapshotKeepEntries int // 快照后保留的日志条目数
}

func (g *GlobalConfig) Init() {
	// 尝试多个可能的路径
	paths := []string{
		"config/config.json",       // 从项目根目录运行
		"../config/config.json",    // 从 cmd/Server 或 cmd/client 运行
		"../../config/config.json", // 从更深层目录运行
		"config.json",              // 当前目录
	}

	var data []byte
	var err error

	for _, path := range paths {
		data, err = os.ReadFile(path)
		if err == nil {
			slog.Info("config file found", "path", path)
			break
		}
	}

	if err != nil {
		slog.Error("failed to read config", "error", err)
		slog.Warn("falling back to default config")
		return // 使用默认配置，不退出
	}

	err = json.Unmarshal(data, g)
	if err != nil {
		slog.Error("failed to parse config", "error", err)
		return
	}

	slog.Info("config initialized")
}

func NewGlobalConfig() *GlobalConfig {
	logDir := defaultLogDir()
	global := &GlobalConfig{

		Name:                    "Raft",
		Port:                    8080,
		Host:                    "localhost",
		Version:                 "1.0.0",
		MaxConn:                 1000,
		MaxPackageSize:          1024,
		WorkerPoolSize:          10,
		MaxWorkerTaskLen:        10000,
		MaxMsgChanLen:           100,
		TcpServer:               nil,
		MaxMemTableP:            0.5,
		MaxMemTableLevel:        32,
		MaxMemTableSize:         1024,
		WALPath:                 filepath.Join(logDir, "wal.log"),
		SSTablePath:             logDir,
		Peers:                   []string{"localhost:8080"}, // 默认单节点
		Me:                      0,                          // 默认节点ID
		RaftSnapshotThreshold:   1000,                       // 默认快照阈值
		RaftSnapshotKeepEntries: 100,                        // 默认保留条目数
	}
	global.Init()
	global.ParseFlags()
	return global
}

func defaultLogDir() string {
	wd, err := os.Getwd()
	if err != nil {
		return "log"
	}

	for dir := wd; ; dir = filepath.Dir(dir) {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return filepath.Join(dir, "log")
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
	}

	return "log"
}

// ParseFlags 解析命令行参数
func (g *GlobalConfig) ParseFlags() {
	// 创建一个新的 FlagSet，避免与全局的 CommandLine 冲突
	fs := flag.NewFlagSet("bandb", flag.ContinueOnError)
	fs.Usage = func() {}

	// 定义命令行参数
	meFlag := fs.Int("me", -1, "Current node index in peers list")

	// 解析命令行参数，忽略未定义的参数
	err := fs.Parse(meFlagArgs(os.Args[1:]))
	if err != nil {
		// 忽略错误，继续执行
	}

	// 处理命令行参数
	if *meFlag >= 0 {
		g.Me = *meFlag
		slog.Info("me set via flag", "me", g.Me)
	}

	// 处理环境变量（优先级低于命令行参数）
	if g.Me < 0 {
		if meEnv := os.Getenv("RAFT_ME"); meEnv != "" {
			if meInt, err := strconv.Atoi(meEnv); err == nil {
				g.Me = meInt
				slog.Info("me set via env", "me", g.Me)
			}
		}
	}

	// 验证配置
	if g.Me < 0 || g.Me >= len(g.Peers) {
		slog.Error("invalid me value", "me", g.Me, "peers_len", len(g.Peers))
		panic("invalid me value")
	}

	slog.Info("config finalized", "peers", g.Peers, "me", g.Me)
}

func meFlagArgs(args []string) []string {
	filtered := make([]string, 0, 2)
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "-me" {
			filtered = append(filtered, arg)
			if i+1 < len(args) {
				filtered = append(filtered, args[i+1])
				i++
			}
			continue
		}
		if strings.HasPrefix(arg, "-me=") {
			filtered = append(filtered, arg)
		}
	}
	return filtered
}

var G = NewGlobalConfig()
