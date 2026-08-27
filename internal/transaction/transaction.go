package transaction

import (
	"encoding/json"
	"fmt"
	"maelstrom/internal/retry"
	"maps"
	"os"
	"strconv"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var (
	forceAbortKey    int
	forceAbortActive bool
)

type config struct {
	node         *maelstrom.Node
	retryHandler retry.RetryHandler

	store map[int]int

	inputChan chan inputChanMsg
}

func newConfig(node *maelstrom.Node, retryHandler retry.RetryHandler) *config {
	inputChan := make(chan inputChanMsg)
	config := &config{
		node:         node,
		retryHandler: retryHandler,
		store:        make(map[int]int),
		inputChan:    inputChan,
	}
	go config.processTxn()

	// Introducing transaction abort
	if v := os.Getenv("TXN_FORCE_ABORT_KEY"); v != "" {
		if k, err := strconv.Atoi(v); err == nil {
			forceAbortKey = k
			forceAbortActive = true
		}
	}
	return config
}

func Handle(node *maelstrom.Node, retryHandler retry.RetryHandler) {
	config := newConfig(node, retryHandler)
	node.Handle("txn", config.txn)
	node.Handle("txn-update", config.txnUpdate)
}

func (c *config) txn(msg maelstrom.Message) error {
	body := input{}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return fmt.Errorf("TXN: Error while decoding json: %s", err)
	}

	outputChan := make(chan outputChanMsg)
	defer close(outputChan)
	c.inputChan <- inputChanMsg{input: body, src: msg.Src, ch: outputChan, shouldBroadcast: true}

	processedMsg := <-outputChan
	if processedMsg.err != nil {
		return c.node.Reply(msg, output{Type: "error", Code: maelstrom.TxnConflict, Text: "txn abort"})
	}
	return c.node.Reply(msg, output{Type: "txn_ok", Txn: processedMsg.txns})
}

func (c *config) txnUpdate(msg maelstrom.Message) error {
	body := input{}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return fmt.Errorf("TXN: Error while decoding json: %s", err)
	}

	outputChan := make(chan outputChanMsg)
	defer close(outputChan)
	c.inputChan <- inputChanMsg{input: body, src: msg.Src, ch: outputChan, shouldBroadcast: false}

	processedMsg := <-outputChan
	if processedMsg.err != nil {
		return c.node.Reply(msg, output{Type: "error", Code: maelstrom.TxnConflict, Text: "txn abort"})
	}
	return c.node.Reply(msg, output{Type: "txn_ok", Txn: processedMsg.txns})
}

func (c *config) processTxn() {
	for {
		msg := <-c.inputChan
		pendingWrites := make(map[int]int)
		writtenTxns := make([]any, 0)
		aborted := false
		for i, t := range msg.Txns {
			txn := t.([]any)
			op := txn[0].(string)
			key := int(txn[1].(float64))

			if op == "r" {
				if v, ok := pendingWrites[key]; ok {
					txn[2] = v
				} else {
					txn[2] = c.store[key]
				}
			} else {
				val := txn[2].(float64)
				pendingWrites[key] = int(val)

				writeTxn := make([]any, len(txn))
				copy(writeTxn, txn)
				writtenTxns = append(writtenTxns, writeTxn)
			}
			msg.Txns[i] = txn

			if isAborted(key) {
				aborted = true
				break
			}
		}

		if aborted == true {
			msg.ch <- outputChanMsg{err: fmt.Errorf("Transaction abort")}
			continue
		}

		maps.Copy(c.store, pendingWrites)

		if msg.shouldBroadcast && len(writtenTxns) > 0 {
			for _, id := range c.node.NodeIDs() {
				if id == c.node.ID() || id == msg.src {
					continue
				}

				c.retryHandler.Send(id, "txn-update", writtenTxns...)
			}
		}
		msg.ch <- outputChanMsg{txns: msg.Txns}
	}
}

// isAborted() is a test function which is used to check for G1a(aborted reads)
// On true, this ensures the transaction is rolled back and the values are not updated in the store
func isAborted(key int) bool {
	return forceAbortActive && key == forceAbortKey
}
