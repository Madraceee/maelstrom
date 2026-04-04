package main

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type broadcaster struct {
	mu         *sync.RWMutex
	dstConnMap map[string]bool
	node       *maelstrom.Node
	store      *store
}

func NewBroadcaster(node *maelstrom.Node, store *store) *broadcaster {
	return &broadcaster{
		mu:         &sync.RWMutex{},
		dstConnMap: make(map[string]bool),
		node:       node,
		store:      store,
	}
}

func (b *broadcaster) Send(dst string) {
	b.mu.RLock()
	isConnPresent := b.dstConnMap[dst]
	b.mu.RUnlock()

	if isConnPresent == true {
		return
	}

	b.mu.Lock()
	b.dstConnMap[dst] = true
	b.mu.Unlock()
	go func() {
		err := b.broadcastValues(dst)
		backoff := 100
		for err != nil {
			time.Sleep(time.Millisecond * time.Duration(backoff))
			log.Printf("Trying to send from %s to %s", b.node.ID(), dst)
			err = b.broadcastValues(dst)
			backoff = backoff * 2
		}

		b.mu.Lock()
		b.dstConnMap[dst] = false
		b.mu.Unlock()
	}()
}

func (b *broadcaster) broadcastValues(dst string) error {
	nodeId := b.node.ID()
	err := b.node.RPC(dst, map[string]interface{}{"type": "read"}, func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return fmt.Errorf("BROADCAST: error while reading 'read' from node %s: %v", dst, err)
		}

		recvValues, ok := body["messages"].([]interface{})
		if !ok {
			return fmt.Errorf("BROADCAST: error while getting messages from 'read' from node %s", dst)
		}

		recvIntValues := make([]int, len(recvValues))
		for i, val := range recvValues {
			recvIntValues[i] = int(val.(float64))
		}

		values := b.store.Get()
		missingValues := GetMissingValues(values, recvIntValues)

		for _, val := range missingValues {
			err := b.node.RPC(dst, map[string]interface{}{"type": "broadcast", "message": val}, func(msg maelstrom.Message) error {
				return nil
			})
			if err != nil {
				return fmt.Errorf("BROADCAST: error while sending broadcast to %s: %s", dst, err)
			}
		}

		return nil
	})

	if err != nil {
		return fmt.Errorf("BROADCAST: error id %s sending read message to %s : %s", nodeId, dst, err.Error())
	}

	return nil
}

func GetMissingValues(src, dest []int) []int {
	missingValues := make([]int, 0)
	for _, value1 := range src {
		isPresent := false
		for _, value2 := range dest {
			if value1 == value2 {
				isPresent = true
			}
		}

		if isPresent == false {
			missingValues = append(missingValues, value1)
		}
	}

	return missingValues
}
