package model

const (
	TaskKindExecution    = "EXECUTION"
	TaskKindCoordination = "COORDINATION"
)

var validTaskKinds = map[string]struct{}{
	TaskKindExecution:    {},
	TaskKindCoordination: {},
}

func ValidTaskKind(kind string) bool {
	_, ok := validTaskKinds[kind]
	return ok
}
