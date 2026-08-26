package generate

import (
	"strconv"
	"strings"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const (
	epoch          = int64(1672531200000)
	seqBits        = int64(12)
	machineIdBits      = int64(10)
	maxSeqBits     = int64(-1) ^ (int64(-1) << seqBits)
	maxMachineBits = int64(-1) ^ (int64(-1) << machineIdBits)
	machineIdShift = seqBits
	timestampShift = seqBits + machineIdBits
)

type generator struct {
	mu        sync.Mutex
	seq       int64
	timestamp int64
}

func Handle(node *maelstrom.Node) {
	gen := &generator{
		mu:        sync.Mutex{},
		seq:       0,
		timestamp: time.Now().UnixMilli(),
	}

	node.Handle("generate", func(msg maelstrom.Message) error {
		reply := gen.generate(msg, node)
		return node.Reply(msg, reply)
	})
}

func (g *generator) generate(m maelstrom.Message, node *maelstrom.Node) any {
	return map[string]interface{}{
		"type": "generate_ok", 
		"id": strconv.FormatInt(g.GetID(node.ID()),10),
	}
}

func (g *generator) GetID(nodeId string) int64 {
	g.mu.Lock()
	defer g.mu.Unlock()

	machineId := getMachineId(nodeId)
	currTimestamp := time.Now().UnixMilli()

	currTimestamp = max(currTimestamp, g.timestamp)

	if currTimestamp == g.timestamp {
		g.seq = (g.seq + 1) & maxSeqBits

		if g.seq == 0 {
			for currTimestamp <= g.timestamp {
				currTimestamp = time.Now().UnixMilli()
			}
		}
	} else {
		g.seq = 0
	}

	g.timestamp = currTimestamp

	id := ((currTimestamp - epoch) << timestampShift) | (machineId << machineIdShift) | g.seq

	return id
}

func getMachineId(id string) int64 {
	n, err := strconv.Atoi(strings.TrimPrefix(id, "n"))
	if err != nil {
		panic(err)
	}
	return int64(n) & maxMachineBits
}
