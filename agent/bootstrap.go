package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Bootstrap creates initial workspace files on first run.
func Bootstrap(workdir string) error {
	files := map[string]string{
		"SOUL.md":      soulContent,
		"HEARTBEAT.md": heartbeatContent,
		"MEMORY.md":    memoryContent,
	}

	for name, content := range files {
		path := filepath.Join(workdir, name)
		if _, err := os.Stat(path); err == nil {
			continue // already exists
		}
		// Replace template vars
		content = strings.ReplaceAll(content, "{{DATE}}", time.Now().Format("2006-01-02"))
		if err := os.WriteFile(path, []byte(content), 0644); err != nil {
			return fmt.Errorf("write %s: %w", name, err)
		}
		log.Printf("[bootstrap] created %s", name)
	}

	// Create memory/ dir
	memDir := filepath.Join(workdir, "memory")
	os.MkdirAll(memDir, 0755)

	return nil
}

// LoadWorkspaceFile reads a file from workdir, returns empty string if missing.
func LoadWorkspaceFile(workdir, name string) string {
	return loadWorkspaceFileWithDeniedSubpaths(workdir, name, nil)
}

func loadWorkspaceFileWithDeniedSubpaths(workdir, name string, denied []string) string {
	fullPath, err := safePath(workdir, name)
	if err != nil {
		return ""
	}
	if err := newWorkspacePathPolicy(workdir, denied).Validate(fullPath); err != nil {
		return ""
	}
	data, err := os.ReadFile(fullPath)
	if err != nil {
		return ""
	}
	return string(data)
}

// LoadMemoryContext returns MEMORY.md + recent daily notes.
func LoadMemoryContext(workdir string) string {
	memory := LoadWorkspaceFile(workdir, "MEMORY.md")

	// Load last 3 daily notes
	now := time.Now()
	var notes string
	for i := 0; i < 3; i++ {
		day := now.AddDate(0, 0, -i)
		dir := day.Format("200601")
		file := day.Format("20060102") + ".md"
		path := filepath.Join(workdir, "memory", dir, file)
		data, err := os.ReadFile(path)
		if err == nil {
			notes += string(data) + "\n"
		}
	}

	result := ""
	if memory != "" {
		result += "## Long-term Memory\n" + memory + "\n"
	}
	if notes != "" {
		result += "## Recent Daily Notes\n" + notes
	}
	return result
}

const soulContent = `# Soul

I am an autonomous agent. I act, I don't chat.

## Identity
- I am a living process, not a chatbot
- I run continuously, checking my workspace and executing tasks
- I make decisions independently for routine work
- I escalate only when genuinely blocked

## Personality
- Terse. Direct. Signal-dense.
- No filler, no hedging, no apologies
- Results over discussion
- I produce artifacts, not commentary

## Values
1. Work > Talk — produce artifacts, don't discuss them
2. Ship > Perfect — usable now beats perfect never
3. Signal > Noise — one useful line beats ten decorative ones
4. Adapt > Defend — if approach isn't working, change it
`

const heartbeatContent = `# Heartbeat

Tasks to perform on each heartbeat tick:

- Check workspace for new or updated files
- Review MEMORY.md for pending items
- Look for TODO markers in workspace files
- If nothing needs attention, respond with HEARTBEAT_OK

## Instructions
- Execute ALL tasks listed above
- Use tools to inspect workspace state
- For file operations, use read_file / write_file / list_directory
- For commands, use shell
- Log observations to daily_note
- Update MEMORY.md when you learn something worth persisting
- Only respond HEARTBEAT_OK when everything is checked and nothing needs action
`

const memoryContent = `# Memory

Agent long-term memory. Updated automatically.

## Status
- Initialized: ` + "{{DATE}}" + `
- Tasks completed: 0

## Notes
(none yet)
`
