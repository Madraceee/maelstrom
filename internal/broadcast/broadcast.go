package broadcast

import (
	"encoding/json"
	"fmt"
	"maelstrom/internal/broadcaster"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type broadcastOptions struct {
	node        *maelstrom.Node
	topology    map[string][]string
	broadcaster broadcaster.Broadcaster
	store       *store
}

func newBroadCastOptions(node *maelstrom.Node, broadcaster broadcaster.Broadcaster, store *store) *broadcastOptions {
	return &broadcastOptions{
		node:        node,
		topology:    make(map[string][]string),
		broadcaster: broadcaster,
		store:       store,
	}
}

func Handle(node *maelstrom.Node, broadcaster broadcaster.Broadcaster) {
	store := NewStore()
	bopt := newBroadCastOptions(node, broadcaster, store)
	node.Handle("broadcast", bopt.broadcast)
	node.Handle("topology", bopt.SetTopology)
	node.Handle("read", bopt.read)
	node.Handle("broadcast-group", bopt.broadcastGroup)
}

func (b *broadcastOptions) broadcast(msg maelstrom.Message) error {
	body := broadcastMsg{}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return fmt.Errorf("BROADCAST: Error while decoding json: %s", err)
	}

	b.store.Store(body.Message)
	nodeId := b.node.ID()
	for _, connctedNode := range b.topology[nodeId] {
		if connctedNode == msg.Src || connctedNode == nodeId {
			continue
		}
		b.broadcaster.Send(connctedNode, "broadcast-group", body.Message)
	}
	return b.node.Reply(msg, reply{Type: "broadcast_ok"})
}

func (b *broadcastOptions) SetTopology(msg maelstrom.Message) error {
	body := topologyMsg{}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return fmt.Errorf("TOPOLOGY: Error while decoding json: %s", err)
	}

	b.topology = body.Topology

	return b.node.Reply(msg, reply{Type: "topology_ok"})
}

func (b *broadcastOptions) read(msg maelstrom.Message) error {
	return b.node.Reply(msg, reply{Type: "read_ok", Messages: b.store.Get()})
}

func (b *broadcastOptions) broadcastGroup(msg maelstrom.Message) error {
	body := broadcastGrpMsg{}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return fmt.Errorf("BROADCAST: Error while decoding json: %s", err)
	}

	body.Message = b.store.StoreMultiple(body.Message)
	messages := make([]any, len(body.Message))
	for i, msg := range body.Message{
		messages[i] = msg
	}


	nodeId := b.node.ID()
	for _, connctedNode := range b.topology[nodeId] {
		if connctedNode == msg.Src {
			continue
		}
		b.broadcaster.Send(connctedNode, "broadcast-group", messages...)
	}
	return b.node.Reply(msg, reply{Type: "boradcast_ok"})
}
