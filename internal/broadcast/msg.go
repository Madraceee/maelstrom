package broadcast

type broadcastMsg struct {
	Message int `json:"message"`
}

type topologyMsg struct {
	Topology map[string][]string `json:"topology"`
}

type broadcastGrpMsg struct {
	Message []int `json:"message"`
}

type reply struct {
	Type     string `json:"type"`
	Messages []int  `json:"messages,omitzero"`
}
