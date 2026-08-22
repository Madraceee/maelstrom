package availability

import (
	"encoding/json"
	"fmt"
	"maelstrom/internal/broadcaster"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type config struct {
	node        *maelstrom.Node
	broadcaster broadcaster.Broadcaster

	store    map[int]int
	mu       *sync.RWMutex
	isLocked bool
}

func newConfig(node *maelstrom.Node, broadcaster broadcaster.Broadcaster) *config {
	return &config{
		node:        node,
		broadcaster: broadcaster,
		store:       make(map[int]int),
		mu:          &sync.RWMutex{},
		isLocked:    false,
	}
}

func Handle(node *maelstrom.Node, broadcaster broadcaster.Broadcaster) {
	config := newConfig(node, broadcaster)
	node.Handle("txn", config.txn)
	node.Handle("txn-update", config.txnUpdate)
}

func (c *config) txn(msg maelstrom.Message) error {
	body := input{} 
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return fmt.Errorf("TXN: Error while decoding json: %s", err)
	}

	if c.isLocked == true {
		return c.node.Reply(msg, newError(maelstrom.TxnConflict))
	}

	c.mu.Lock()
	c.isLocked = true
	writtenTxns := make([]any, 0)

	for i, t := range body.Txns {
		txn := t.([]any)
		op := txn[0].(string)
		key := txn[1].(float64)

		if op == "r" {
			txn[2] = c.store[int(key)]
		} else {
			val := txn[2].(float64)
			c.store[int(key)] = int(val)

			writeTxn := make([]any, len(txn))
			copy(writeTxn, txn)
			writtenTxns = append(writtenTxns, writeTxn)
		}

		body.Txns[i] = txn
	}

	for _, id := range c.node.NodeIDs() {
		if id == c.node.ID() || id == msg.Src {
			continue
		}

		c.broadcaster.Send(id, "txn-update", writtenTxns...)
	}
	c.isLocked = false
	c.mu.Unlock()

	return c.node.Reply(msg, output{Type: "txn_ok", Txn: body.Txns})
}

func (c *config) txnUpdate(msg maelstrom.Message) error {
	body := input{} 
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return fmt.Errorf("TXN: Error while decoding json: %s", err)
	}

	c.mu.Lock()
	for i, t := range body.Txns {
		txn := t.([]any)
		op := txn[0].(string)
		if op == "r" {
			continue
		}
		key := txn[1].(float64)
		val := txn[2].(float64)
		c.store[int(key)] = int(val)
		body.Txns[i] = txn
	}
	c.mu.Unlock()

	return c.node.Reply(msg, output{Type: "txn_ok", Txn: body.Txns})
}
