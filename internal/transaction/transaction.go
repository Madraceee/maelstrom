package transaction

import (
	"encoding/json"
	"fmt"
	"maelstrom/internal/broadcaster"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type config struct {
	node        *maelstrom.Node
	broadcaster broadcaster.Broadcaster

	store    map[int]int

	inputChan chan inputChanMsg
}

func newConfig(node *maelstrom.Node, broadcaster broadcaster.Broadcaster) *config {
	inputChan := make(chan inputChanMsg)
	config := &config{
		node:        node,
		broadcaster: broadcaster,
		store:       make(map[int]int),
		inputChan:   inputChan,
	}
	go config.processTxn()
	return config
}

func Handle(node *maelstrom.Node, broadcaster broadcaster.Broadcaster) {
	config := newConfig(node, broadcaster)
	node.Handle("txn", config.txnNew)
	node.Handle("txn-update", config.txnUpdateNew)
}

func (c *config) txnNew(msg maelstrom.Message) error {
	body := input{}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return fmt.Errorf("TXN: Error while decoding json: %s", err)
	}

	outputChan := make(chan []any)
	c.inputChan <- inputChanMsg{input: body, src: msg.Src, ch: outputChan, shouldBroadcast: true}

	processedMsg := <-outputChan
	close(outputChan)
	return c.node.Reply(msg, output{Type: "txn_ok", Txn: processedMsg})
}

func (c *config) txnUpdateNew(msg maelstrom.Message) error {
	body := input{}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return fmt.Errorf("TXN: Error while decoding json: %s", err)
	}

	outputChan := make(chan []any)
	c.inputChan <- inputChanMsg{input: body, src: msg.Src, ch: outputChan, shouldBroadcast: false}

	processedMsg := <-outputChan
	close(outputChan)
	return c.node.Reply(msg, output{Type: "txn_ok", Txn: processedMsg})
}

func (c *config) processTxn() {
	for {
		txns := <-c.inputChan
		writtenTxns := make([]any, 0)
		for i, t := range txns.Txns {
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
			txns.Txns[i] = txn
		}
		if txns.shouldBroadcast {
			for _, id := range c.node.NodeIDs() {
				if id == c.node.ID() || id == txns.src {
					continue
				}

				c.broadcaster.Send(id, "txn-update", writtenTxns...)
			}
		}
		txns.ch <- txns.Txns
	}
}
