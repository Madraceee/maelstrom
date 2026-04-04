package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type generator struct {
	mu        sync.Mutex
	count     int
	timestamp int64
}

type store struct {
	mu    sync.RWMutex
	cache map[int]interface{}
	store []int
}

func (g *generator) GetID(nodeId string) string {
	g.mu.Lock()
	defer g.mu.Unlock()

	currTimestamp := time.Now().UnixMilli()
	if currTimestamp >= g.timestamp {
		g.timestamp = currTimestamp
	}

	g.count += 1
	return fmt.Sprintf("%s-%d%d", nodeId, g.timestamp, g.count)
}

func (s *store) Store(value int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.cache[value]; ok {
		return
	}
	s.store = append(s.store, value)
	s.cache[value] = struct{}{}
}

func (s *store) Get() []int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	storeCopy := s.store[:]
	return storeCopy
}

func BroadCastValues(node *maelstrom.Node, store *store, dst string, msg maelstrom.Message) error {
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

	values := store.Get()
	if len(values) == len(recvIntValues) {
		return nil
	}
	missingValues := GetMissingValues(values, recvIntValues)

	for _, val := range missingValues {
		err := node.RPC(dst, map[string]interface{}{"type": "broadcast", "message": val}, func(msg maelstrom.Message) error {
			log.Printf("Recevied message id %s from %s for val %v", node.ID(), dst, val)
			return nil
		})
		if err != nil {
			return fmt.Errorf("BROADCAST: error while sending broadcast to %s: %s", dst, err)
		}
	}

	return nil
}

func main() {
	node := maelstrom.NewNode()
	topology := make(map[string][]string)

	gen := &generator{
		mu:        sync.Mutex{},
		count:     0,
		timestamp: time.Now().UnixMilli(),
	}

	store := &store{
		mu:    sync.RWMutex{},
		store: make([]int, 0),
		cache: make(map[int]interface{}),
	}

	node.Handle("echo", func(msg maelstrom.Message) error {
		body := make(map[string]interface{})
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return fmt.Errorf("ECHO: Error while decoding json: %s", err)
		}

		value, ok := body["echo"]
		if !ok {
			return fmt.Errorf("ECHO: Body does not have echo")
		}
		return node.Reply(msg, map[string]interface{}{"type": "echo_ok", "echo": value})
	})

	node.Handle("generate", func(msg maelstrom.Message) error {
		return node.Reply(msg, map[string]interface{}{"type": "generate_ok", "id": gen.GetID(node.ID())})
	})

	node.Handle("broadcast", func(msg maelstrom.Message) error {
		body := make(map[string]interface{})
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return fmt.Errorf("BROADCAST: Error while decoding json: %s", err)
		}

		value, ok := body["message"].(float64)
		if !ok {
			return fmt.Errorf("BROADCAST: Body does not have message")
		}
		store.Store(int(value))

		nodeId := node.ID()

		for _, connctedNode := range topology[nodeId] {
			if connctedNode == msg.Dest {
				continue
			}

			go func() error {
				msg, err := node.SyncRPC(context.TODO(), connctedNode, map[string]interface{}{"type": "read"})
				for err != nil {
					return fmt.Errorf("BROADCAST: error id %s sending read message to %s : %s", nodeId, connctedNode, err.Error())
				}

				if err := BroadCastValues(node, store, connctedNode, msg); err != nil {
					return fmt.Errorf("BROADCAST: %s", err)
				}
				return nil
			}()
		}
		return node.Reply(msg, map[string]interface{}{"type": "broadcast_ok"})
	})

	node.Handle("read", func(msg maelstrom.Message) error {
		return node.Reply(msg, map[string]interface{}{"type": "read_ok", "messages": store.Get()})
	})

	node.Handle("topology", func(msg maelstrom.Message) error {
		body := make(map[string]interface{})
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return fmt.Errorf("TOPOLOGY: Error while decoding json: %s", err)
		}

		recvTopology, ok := body["topology"].(map[string]interface{})
		if !ok {
			return fmt.Errorf("TOPOLOGY: Body does not have topology")
		}

		for k, v := range recvTopology {
			nodes := v.([]interface{})
			topology[k] = make([]string, len(nodes))
			for i, node := range nodes {
				topology[k][i] = node.(string)
			}
		}

		return node.Reply(msg, map[string]interface{}{"type": "topology_ok"})
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
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
