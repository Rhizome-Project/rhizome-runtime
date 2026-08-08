package model

const (
	TaskLinkBlocks    = "BLOCKS"
	TaskLinkRelatesTo = "RELATES_TO"
	TaskLinkSubtaskOf = "SUBTASK_OF"
)

var validTaskLinkTypes = map[string]struct{}{
	TaskLinkBlocks:    {},
	TaskLinkRelatesTo: {},
	TaskLinkSubtaskOf: {},
}

func ValidTaskLinkType(linkType string) bool {
	_, ok := validTaskLinkTypes[linkType]
	return ok
}
