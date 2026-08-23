package kafka

type sendInput struct {
	Type string `json:"type"`
	Key  string `json:"key"`
	Msg  int    `json:"msg"`
}

type sendOutput struct {
	Type   string `json:"type"`
	Offset int    `json:"offset"`
}

type pollInput struct {
	Type    string         `json:"type"`
	Offsets map[string]int `json:"offsets"`
}

type pollOutput struct {
	Type string              `json:"type"`
	Msgs map[string][][2]int `json:"msgs"`
}

type commitOffsetsInput struct {
	Type    string         `json:"type"`
	Offsets map[string]int `json:"offsets"`
}

type commitOffsetsOutput struct {
	Type string `json:"type"`
}

type listCommitOffsetsInput struct {
	Type string   `json:"type"`
	Keys []string `json:"keys"`
}

type listCommitOffsetsOutput struct {
	Type    string         `json:"type"`
	Offsets map[string]int `json:"offsets"`
}
