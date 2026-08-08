package server

import "github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"

func sqliteErrorPermission(err error) *RPCError {
	if sqlite.IsRSPRolloutDisabledError(err) {
		return &RPCError{Code: errCodePermissionDenied, Message: err.Error()}
	}
	return nil
}
