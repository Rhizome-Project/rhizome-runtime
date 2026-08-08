package server

import (
	"embed"
)

//go:embed assets/*
var AssetsFS embed.FS
