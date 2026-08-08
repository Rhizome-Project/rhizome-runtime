package sqlite

import (
	"embed"
	"io/fs"
	"path"
	"path/filepath"
	"sort"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

func migrationFileNames() ([]string, error) {
	entries, err := fs.ReadDir(migrationFiles, "migrations")
	if err != nil {
		return nil, err
	}

	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if filepath.Ext(entry.Name()) != ".sql" {
			continue
		}
		names = append(names, entry.Name())
	}
	sort.Strings(names)
	return names, nil
}

func migrationSQL(name string) (string, error) {
	raw, err := migrationFiles.ReadFile(path.Join("migrations", name))
	if err != nil {
		return "", err
	}
	return string(raw), nil
}
