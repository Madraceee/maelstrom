package broadcast

import (
	"encoding/json"
	"fmt"
	"maelstrom/internal/retry"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type config struct {
	node        *maelstrom.Node
	topology    map[string][]string
	retryHandler retry.RetryHandler 
	store       *store
}

func newConfig(node *maelstrom.Node, retryHandler retry.RetryHandler, store *store) *config {
	return &config{
		node:        node,
		topology:    make(map[string][]string),
		retryHandler: retryHandler,
		store:       store,
	}
}

func Handle(node *maelstrom.Node, retryHandler retry.RetryHandler) {
	store := NewStore()
	bopt := newConfig(node, retryHandler, store)
	node.Handle("broadcast", bopt.broadcast)
	node.Handle("topology", bopt.setTopology)
	node.Handle("read", bopt.read)
	node.Handle("broadcast-group", bopt.broadcastGroup)
}

func (c *config) broadcast(msg maelstrom.Message) error {
	body := broadcastMsg{}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return fmt.Errorf("BROADCAST: Error while decoding json: %s", err)
	}

	c.store.Store(body.Message)
	nodeId := c.node.ID()
	for _, connctedNode := range c.topology[nodeId] {
		if connctedNode == msg.Src || connctedNode == nodeId {
			continue
		}
		c.retryHandler.Send(connctedNode, "broadcast-group", body.Message)
	}
	return c.node.Reply(msg, reply{Type: "broadcast_ok"})
}

func (c *config) setTopology(msg maelstrom.Message) error {
	body := topologyMsg{}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return fmt.Errorf("TOPOLOGY: Error while decoding json: %s", err)
	}

	c.topology = body.Topology

	return c.node.Reply(msg, reply{Type: "topology_ok"})
}

func (c *config) read(msg maelstrom.Message) error {
	return c.node.Reply(msg, reply{Type: "read_ok", Messages: c.store.Get()})
}

func (c *config) broadcastGroup(msg maelstrom.Message) error {
	body := broadcastGrpMsg{}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return fmt.Errorf("BROADCAST: Error while decoding json: %s", err)
	}

	body.Message = c.store.StoreMultiple(body.Message)
	messages := make([]any, len(body.Message))
	for i, msg := range body.Message {
		messages[i] = msg
	}

	if len(messages) == 0 {
		return c.node.Reply(msg, reply{Type: "broadcast_ok"})
	}

	nodeId := c.node.ID()
	for _, connectedNode := range c.topology[nodeId] {
		if connectedNode == msg.Src {
			continue
		}
		c.retryHandler.Send(connectedNode, "broadcast-group", messages...)
	}
	return c.node.Reply(msg, reply{Type: "broadcast_ok"})
}
