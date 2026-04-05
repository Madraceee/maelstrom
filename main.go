package main

import (
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
	cache map[int]bool
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

	if isPresent := s.cache[value]; isPresent {
		return
	}
	s.store = append(s.store, value)
	s.cache[value] = true
}

func (s *store) StoreMultiple(values []int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, value := range values {
		if isPresent := s.cache[value]; isPresent {
			continue
		}
		s.store = append(s.store, value)
		s.cache[value] = true
	}
}

func (s *store) Get() []int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	storeCopy := make([]int, len(s.store))
	copy(storeCopy, s.store)
	return storeCopy
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
		cache: make(map[int]bool),
	}
	broadcaster := NewBroadcaster(node, store)

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
			if connctedNode == msg.Src {
				continue
			}
			broadcaster.Send(connctedNode, int(value))
		}
		return node.Reply(msg, map[string]interface{}{"type": "broadcast_ok"})
	})

	node.Handle("broadcast-group", func(msg maelstrom.Message) error {
		body := make(map[string]interface{})
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return fmt.Errorf("BROADCAST: Error while decoding json: %s", err)
		}

		values, ok := body["message"].([]interface{})
		if !ok {
			return fmt.Errorf("BROADCAST: Body does not have message")
		}

		intValues := make([]int, len(values))
		for i, value := range values {
			intValues[i] = int(value.(float64))
		}
		store.StoreMultiple(intValues)

		nodeId := node.ID()
		for _, connctedNode := range topology[nodeId] {
			if connctedNode == msg.Src {
				continue
			}
			broadcaster.Send(connctedNode, intValues...)
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
