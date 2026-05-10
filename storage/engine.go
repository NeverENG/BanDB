package storage

import (
	"sync"

	"github.com/NeverENG/BanDB/config"
	"github.com/NeverENG/BanDB/storage/istorage"
)

type StorageCommand struct {
	Type  string
	Key   []byte
	Value []byte
}

type Engine struct {
	memTable istorage.IMemTable
	mu       sync.RWMutex
	applyCh  chan StorageCommand
}

func NewEngine(memTable istorage.IMemTable) *Engine {
	e := &Engine{
		memTable: memTable,
		applyCh:  make(chan StorageCommand, 100),
	}
	go e.applyWorker()
	return e
}

func (e *Engine) Put(key []byte, value []byte) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	err := e.memTable.Put(key, value)
	if err != nil {
		return err
	}
	if e.memTable.Size() > config.G.MaxMemTableSize {
		e.memTable.StartFlush()
	}
	return nil
}

func (e *Engine) Get(key []byte) ([]byte, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	return e.memTable.Get(key)
}

func (e *Engine) Delete(key []byte) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	return e.memTable.Delete(key)
}

func (e *Engine) Apply(cmd StorageCommand) error {
	e.applyCh <- cmd
	return nil
}

func (e *Engine) GetApplyCh() chan StorageCommand {
	return e.applyCh
}

func (e *Engine) applyWorker() {
	for cmd := range e.applyCh {
		switch cmd.Type {
		case "Put":
			e.Put(cmd.Key, cmd.Value)
		case "Delete":
			e.Delete(cmd.Key)
		}
	}
}
