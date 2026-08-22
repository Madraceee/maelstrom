package broadcaster

import (
	"context"
	"errors"
	"math"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Broadcaster interface {
	Send(dst string, msgType string, values ...any)
}

type broadcasterOptions struct {
	mu         *sync.RWMutex
	dstConnMap map[string][]any
	isWorking  map[string]bool
	node       *maelstrom.Node
}

func NewBroadcaster(node *maelstrom.Node) Broadcaster {
	return &broadcasterOptions{
		mu:         &sync.RWMutex{},
		dstConnMap: make(map[string][]any),
		isWorking:  make(map[string]bool),
		node:       node,
	}
}

func (b *broadcasterOptions) Send(dst string, msgType string, values ...any) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.dstConnMap[dst]; !ok {
		b.dstConnMap[dst] = make([]any, 0)
	}

	b.dstConnMap[dst] = append(b.dstConnMap[dst], values...)
	isWorking := b.isWorking[dst]

	if !isWorking {
		b.isWorking[dst] = true
		go func() {
			count := 0
			waitTime := 100
			contextTime := 100
			for {
				ctx, cancel := context.WithTimeout(context.TODO(), time.Millisecond*time.Duration(contextTime))
				b.mu.RLock()
				values := b.dstConnMap[dst]
				b.mu.RUnlock()
				_, err := b.node.SyncRPC(ctx, dst, map[string]interface{}{"type": msgType, "message": values})
				cancel()

				if maelstrom.ErrorCode(err) == maelstrom.Timeout || errors.Is(err, context.DeadlineExceeded) {
					count++
					continue
				}

				b.mu.Lock()
				b.dstConnMap[dst] = b.dstConnMap[dst][len(values):]
				remaining := len(b.dstConnMap[dst])
				if remaining == 0 {
					b.isWorking[dst] = false
				}
				b.mu.Unlock()
				if remaining > 0 {
					time.Sleep(time.Millisecond * time.Duration(waitTime*pow(2, count)))
					continue
				}
				return
			}
		}()
	}
}

func pow(x, y int) int {
	return int(math.Pow(float64(x), float64(y)))
}
