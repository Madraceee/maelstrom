package generate

import (
	"fmt"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type generator struct {
	mu        sync.Mutex
	count     int
	timestamp int64
}

func Handle(node *maelstrom.Node) {
	gen := &generator{
		mu:        sync.Mutex{},
		count:     0,
		timestamp: time.Now().UnixMilli(),
	}

	node.Handle("generate", func(msg maelstrom.Message) error {
		reply := gen.generate(msg, node)
		return node.Reply(msg, reply)
	})
}

func (g *generator) generate(m maelstrom.Message, node *maelstrom.Node) any {
	return map[string]interface{}{"type": "generate_ok", "id": g.GetID(node.ID())}
}

func (g *generator) GetID(nodeId string) string {
	g.mu.Lock()
	defer g.mu.Unlock()

	currTimestamp := time.Now().UnixMilli()
	if currTimestamp >= g.timestamp {
		g.timestamp = currTimestamp
	}

	g.count += 1
	return fmt.Sprintf("%d-%d%s", g.timestamp, g.count, nodeId)
}
