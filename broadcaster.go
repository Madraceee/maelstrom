package main

import (
	"context"
	"errors"
	"math"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type broadcaster struct {
	mu         *sync.RWMutex
	dstConnMap map[string][]int
	isWorking  map[string]bool
	node       *maelstrom.Node
	store      *store
}

func NewBroadcaster(node *maelstrom.Node, store *store) *broadcaster {
	return &broadcaster{
		mu:         &sync.RWMutex{},
		dstConnMap: make(map[string][]int),
		isWorking:  make(map[string]bool),
		node:       node,
		store:      store,
	}
}

func (b *broadcaster) Send(dst string, values ...int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.dstConnMap[dst]; !ok {
		b.dstConnMap[dst] = make([]int, 0)
	}

	b.dstConnMap[dst] = append(b.dstConnMap[dst], values...)
	isWorking := b.isWorking[dst]

	if !isWorking {
		b.isWorking[dst] = true
		go func() {
			count := 0
			waitTime := 10
			for {
				time.Sleep(time.Millisecond * time.Duration(waitTime*pow(2, count)))
				contextTime := 1000
				b.mu.RLock()
				values := b.dstConnMap[dst]
				b.mu.RUnlock()
				ctx, cancel := context.WithTimeout(context.TODO(), time.Millisecond*time.Duration(contextTime))
				_, err := b.node.SyncRPC(ctx, dst, map[string]interface{}{"type": "broadcast-group", "message": values})
				cancel()

				if maelstrom.ErrorCode(err) == maelstrom.Timeout || errors.Is(err, context.DeadlineExceeded) {
					count++
					continue
				}

				b.mu.Lock()
				b.dstConnMap[dst] = b.dstConnMap[dst][len(values):]
				b.isWorking[dst] = false
				b.mu.Unlock()
				return
			}
		}()
	}

}

func pow(x, y int) int {
	return int(math.Pow(float64(x), float64(y)))
}
