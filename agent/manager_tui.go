package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	tea "github.com/charmbracelet/bubbletea"
)

type managerActionMsg struct {
	text          string
	refresh       bool
	loadLogs      bool
	logTail       []string
	logTitle      string
	live          *managerLiveRuntimeStatus
	catalog       *managerWorkspaceCatalog
	quit          bool
	chatAgent     string
	attach        string
	selectWorkdir string
}

type managerLogTickMsg struct{}

type managerLiveRuntimeStatus struct {
	ProcessState    string
	Status          string
	Summary         string
	Paused          bool
	Attachable      bool
	ActiveTaskID    string
	ActiveSessionID string
	ControlMode     string
	LastAction      string
	LastReason      string
	Trigger         string
	FocusTaskID     string
	FocusTensionID  string
	FocusClusterID  string
	WorkType        string
	WorkGateState   string
	WorkGateType    string
	WorkGateReason  string
	WorkGateSummary string
	Error           string
}

type managerWorkspaceCatalog struct {
	Tasks    []WorkspaceTaskRecord
	Tensions []TensionFrontierItem
	Error    string
}

type managerDefaultFieldSpec struct {
	Key    string
	Label  string
	Secret bool
}

type managerAgentFieldSpec struct {
	Key      string
	Label    string
	Action   bool
	Danger   bool
	ReadOnly bool
}

type managerCreateFieldSpec struct {
	Key      string
	Label    string
	Action   bool
	ReadOnly bool
}

type managerPickerMode string

const (
	managerPickerNone    managerPickerMode = ""
	managerPickerTask    managerPickerMode = "task"
	managerPickerTension managerPickerMode = "tension"
)

var (
	managerTensionActions         = []string{"focus", "detach", "lifecycle"}
	managerTensionLifecycleStates = []string{"ACTIVE", "IN_REVIEW", "RESOLVED"}
	managerDefaultFieldSpecs      = []managerDefaultFieldSpec{
		{Key: "host_url", Label: "host"},
		{Key: "workspace_id", Label: "workspace"},
		{Key: "workspace_password", Label: "workspace_password", Secret: true},
		{Key: "owner_user_id", Label: "owner"},
		{Key: "llm_backend", Label: "backend"},
		{Key: "model", Label: "model"},
		{Key: "role", Label: "role"},
		{Key: "capabilities", Label: "capabilities"},
	}
	managerAgentFieldSpecs = []managerAgentFieldSpec{
		{Key: "agent_id", Label: "agent_id", ReadOnly: true},
		{Key: "display_name", Label: "display_name"},
		{Key: "workdir", Label: "workdir"},
		{Key: "__remove__", Label: "remove from registry", Action: true, Danger: true},
	}
	managerCreateFieldSpecs = []managerCreateFieldSpec{
		{Key: "parent_dir", Label: "parent_dir"},
		{Key: "folder_name", Label: "folder_name"},
		{Key: "workdir_preview", Label: "workdir", ReadOnly: true},
		{Key: "__launch__", Label: "launch onboard", Action: true},
	}
)

const (
	managerLogTailLines       = 40
	managerLogRefreshInterval = 2 * time.Second
)

type managerVisorModel struct {
	ctx               context.Context
	registry          BotRegistry
	selected          int
	width             int
	height            int
	commandMode       bool
	command           string
	message           string
	showLogs          bool
	logTitle          string
	logLines          []string
	liveStatus        *managerLiveRuntimeStatus
	catalog           *managerWorkspaceCatalog
	pickerMode        managerPickerMode
	pickerIndex       int
	createMode        bool
	createEditing     bool
	createFieldIndex  int
	createInput       string
	createParentDir   string
	createFolderName  string
	agentMode         bool
	agentEditing      bool
	agentFieldIndex   int
	agentInput        string
	defaultsMode      bool
	defaultsEditing   bool
	defaultsIndex     int
	defaultsInput     string
	taskStatusFilter  string
	tensionTypeFilter string
	tensionAction     string
	tensionLifecycle  string
	pendingChat       string
	pendingAttach     string
}

func newManagerVisorModel(ctx context.Context) managerVisorModel {
	registry := LoadBotRegistry()
	parentDir := defaultManagerCreateParentDir(registry, 0)
	return managerVisorModel{
		ctx:              ctx,
		registry:         registry,
		createParentDir:  parentDir,
		createFolderName: suggestNewAgentFolderName(registry),
		tensionAction:    "focus",
		tensionLifecycle: "ACTIVE",
	}
}

func (m managerVisorModel) Init() tea.Cmd {
	return m.selectedRefreshCmd()
}

func (m managerVisorModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		return m, nil
	case managerActionMsg:
		if msg.text != "" {
			m.message = msg.text
		}
		if msg.refresh {
			m.registry = LoadBotRegistry()
			if msg.selectWorkdir != "" {
				if selected := findManagedAgentIndexByWorkdir(m.registry, msg.selectWorkdir); selected >= 0 {
					m.selected = selected
				}
			}
			if len(m.registry.Agents) == 0 {
				m.selected = 0
			} else if m.selected >= len(m.registry.Agents) {
				m.selected = len(m.registry.Agents) - 1
			}
			if strings.TrimSpace(m.createParentDir) == "" {
				m.createParentDir = defaultManagerCreateParentDir(m.registry, m.selected)
			}
			if strings.TrimSpace(m.createFolderName) == "" {
				m.createFolderName = suggestNewAgentFolderName(m.registry)
			}
		}
		if msg.loadLogs {
			m.showLogs = true
			m.logTitle = msg.logTitle
			m.logLines = append([]string(nil), msg.logTail...)
		}
		if msg.live != nil {
			m.liveStatus = msg.live
		}
		if msg.catalog != nil {
			m.catalog = msg.catalog
			m.clampPickerIndex()
		}
		if msg.chatAgent != "" {
			m.pendingChat = msg.chatAgent
			return m, tea.Quit
		}
		if msg.attach != "" {
			m.pendingAttach = msg.attach
			return m, tea.Quit
		}
		if msg.quit {
			return m, tea.Quit
		}
		if msg.refresh {
			return m, m.selectedRefreshCmd()
		}
		return m, nil
	case managerLogTickMsg:
		if !m.showLogs {
			return m, nil
		}
		return m, tea.Batch(m.loadLogsForSelected(managerLogTailLines), managerLogRefreshCmd())
	case tea.KeyMsg:
		if m.commandMode {
			switch msg.String() {
			case "esc":
				m.commandMode = false
				m.command = ""
				return m, nil
			case "enter":
				cmd := strings.TrimSpace(m.command)
				m.commandMode = false
				m.command = ""
				if cmd == "" {
					return m, nil
				}
				return m, m.executeCommand(cmd)
			case "backspace", "ctrl+h":
				if len(m.command) > 0 {
					_, size := utf8LastRuneInString(m.command)
					m.command = m.command[:len(m.command)-size]
				}
				return m, nil
			default:
				if r := runeFromKeyMsg(msg); r != 0 && !unicode.IsControl(r) {
					m.command += string(r)
				}
				return m, nil
			}
		}
		if m.createMode {
			if m.createEditing {
				switch msg.String() {
				case "esc":
					m.createEditing = false
					m.createInput = ""
					return m, nil
				case "enter":
					spec, _, ok := m.selectedCreateField()
					if !ok {
						m.createEditing = false
						m.createInput = ""
						return m, nil
					}
					value := strings.TrimSpace(m.createInput)
					m.createEditing = false
					m.createInput = ""
					switch spec.Key {
					case "parent_dir":
						m.createParentDir = value
					case "folder_name":
						m.createFolderName = value
					}
					return m, nil
				case "backspace", "ctrl+h":
					if len(m.createInput) > 0 {
						_, size := utf8LastRuneInString(m.createInput)
						m.createInput = m.createInput[:len(m.createInput)-size]
					}
					return m, nil
				case "ctrl+u":
					m.createInput = ""
					return m, nil
				default:
					if r := runeFromKeyMsg(msg); r != 0 && !unicode.IsControl(r) {
						m.createInput += string(r)
					}
					return m, nil
				}
			}
			switch msg.String() {
			case "esc":
				m.createMode = false
				m.createEditing = false
				m.createInput = ""
				return m, nil
			case "up", "k":
				if m.createFieldIndex > 0 {
					m.createFieldIndex--
				}
				return m, nil
			case "down", "j":
				if m.createFieldIndex < len(managerCreateFieldSpecs)-1 {
					m.createFieldIndex++
				}
				return m, nil
			case "enter":
				spec, value, ok := m.selectedCreateField()
				if !ok {
					return m, nil
				}
				if spec.Action {
					m.createMode = false
					m.createEditing = false
					m.createInput = ""
					return m, m.launchNewAgentOnboard()
				}
				if spec.ReadOnly {
					return m, func() tea.Msg { return managerActionMsg{text: spec.Label + " is read-only"} }
				}
				m.createEditing = true
				m.createInput = value
				return m, nil
			}
		}
		if m.agentMode {
			if m.agentEditing {
				switch msg.String() {
				case "esc":
					m.agentEditing = false
					m.agentInput = ""
					return m, nil
				case "enter":
					spec, _, ok := m.selectedAgentField()
					if !ok {
						m.agentEditing = false
						m.agentInput = ""
						return m, nil
					}
					value := strings.TrimSpace(m.agentInput)
					m.agentEditing = false
					m.agentInput = ""
					return m, m.saveSelectedAgentField(spec, value)
				case "backspace", "ctrl+h":
					if len(m.agentInput) > 0 {
						_, size := utf8LastRuneInString(m.agentInput)
						m.agentInput = m.agentInput[:len(m.agentInput)-size]
					}
					return m, nil
				case "ctrl+u":
					m.agentInput = ""
					return m, nil
				default:
					if r := runeFromKeyMsg(msg); r != 0 && !unicode.IsControl(r) {
						m.agentInput += string(r)
					}
					return m, nil
				}
			}
			switch msg.String() {
			case "esc":
				m.agentMode = false
				m.agentEditing = false
				m.agentInput = ""
				return m, nil
			case "up", "k":
				if m.agentFieldIndex > 0 {
					m.agentFieldIndex--
				}
				return m, nil
			case "down", "j":
				if m.agentFieldIndex < len(managerAgentFieldSpecs)-1 {
					m.agentFieldIndex++
				}
				return m, nil
			case "enter":
				spec, value, ok := m.selectedAgentField()
				if !ok {
					return m, nil
				}
				if spec.Action {
					return m, m.saveSelectedAgentField(spec, value)
				}
				if spec.ReadOnly {
					return m, func() tea.Msg {
						record, ok := m.selectedRecord()
						if !ok {
							return managerActionMsg{text: spec.Label + " is read-only"}
						}
						live := loadManagerLiveRuntimeStatus(m.ctx, record)
						catalog := loadManagerWorkspaceCatalog(m.ctx, record)
						return managerActionMsg{
							text:    spec.Label + " is read-only",
							live:    &live,
							catalog: &catalog,
						}
					}
				}
				m.agentEditing = true
				m.agentInput = value
				return m, nil
			}
		}
		if m.defaultsMode {
			if m.defaultsEditing {
				switch msg.String() {
				case "esc":
					m.defaultsEditing = false
					m.defaultsInput = ""
					return m, nil
				case "enter":
					spec, _, ok := m.selectedDefaultField()
					if !ok {
						m.defaultsEditing = false
						m.defaultsInput = ""
						return m, nil
					}
					value := strings.TrimSpace(m.defaultsInput)
					m.defaultsEditing = false
					m.defaultsInput = ""
					return m, m.saveDefaultField(spec.Key, value)
				case "backspace", "ctrl+h":
					if len(m.defaultsInput) > 0 {
						_, size := utf8LastRuneInString(m.defaultsInput)
						m.defaultsInput = m.defaultsInput[:len(m.defaultsInput)-size]
					}
					return m, nil
				case "ctrl+u":
					m.defaultsInput = ""
					return m, nil
				default:
					if r := runeFromKeyMsg(msg); r != 0 && !unicode.IsControl(r) {
						m.defaultsInput += string(r)
					}
					return m, nil
				}
			}
			switch msg.String() {
			case "esc":
				m.defaultsMode = false
				m.defaultsEditing = false
				m.defaultsInput = ""
				return m, nil
			case "up", "k":
				if m.defaultsIndex > 0 {
					m.defaultsIndex--
				}
				return m, nil
			case "down", "j":
				if m.defaultsIndex < len(managerDefaultFieldSpecs)-1 {
					m.defaultsIndex++
				}
				return m, nil
			case "enter":
				_, value, ok := m.selectedDefaultField()
				if !ok {
					return m, nil
				}
				m.defaultsEditing = true
				m.defaultsInput = value
				return m, nil
			}
		}
		if m.pickerMode != managerPickerNone {
			switch msg.String() {
			case "esc":
				m.pickerMode = managerPickerNone
				m.pickerIndex = 0
				return m, nil
			case "f":
				m.message = "task filter: " + m.cycleTaskStatusFilter()
				return m, nil
			case "y":
				m.message = "tension type: " + m.cycleTensionTypeFilter()
				return m, nil
			case "v":
				m.message = "tension action: " + m.cycleTensionAction()
				return m, nil
			case "u":
				m.message = "tension lifecycle: " + m.cycleTensionLifecycle()
				return m, nil
			case "up", "k":
				if m.pickerIndex > 0 {
					m.pickerIndex--
				}
				return m, nil
			case "down", "j":
				if m.pickerIndex < m.currentPickerLen()-1 {
					m.pickerIndex++
				}
				return m, nil
			case "enter":
				cmd := m.confirmPickerSelection()
				m.pickerMode = managerPickerNone
				m.pickerIndex = 0
				return m, cmd
			}
		}

		switch msg.String() {
		case "q", "ctrl+c":
			return m, tea.Quit
		case ":":
			m.commandMode = true
			m.command = ""
			return m, nil
		case "o":
			m.commandMode = true
			m.command = "onboard "
			return m, nil
		case "n":
			m.agentMode = false
			m.agentEditing = false
			m.agentInput = ""
			m.defaultsMode = false
			m.defaultsEditing = false
			m.defaultsInput = ""
			m.pickerMode = managerPickerNone
			m.pickerIndex = 0
			m.createMode = true
			m.createEditing = false
			m.createInput = ""
			if strings.TrimSpace(m.createParentDir) == "" {
				m.createParentDir = defaultManagerCreateParentDir(m.registry, m.selected)
			}
			if strings.TrimSpace(m.createFolderName) == "" {
				m.createFolderName = suggestNewAgentFolderName(m.registry)
			}
			m.clampCreateFieldIndex()
			m.message = "new agent panel opened"
			return m, nil
		case "d":
			m.createMode = false
			m.createEditing = false
			m.createInput = ""
			m.agentMode = false
			m.agentEditing = false
			m.agentInput = ""
			m.defaultsMode = true
			m.defaultsEditing = false
			m.defaultsInput = ""
			m.clampDefaultsIndex()
			m.message = "defaults panel opened"
			return m, nil
		case "e":
			m.createMode = false
			m.createEditing = false
			m.createInput = ""
			m.defaultsMode = false
			m.defaultsEditing = false
			m.defaultsInput = ""
			m.pickerMode = managerPickerNone
			m.pickerIndex = 0
			m.agentMode = true
			m.agentEditing = false
			m.agentInput = ""
			m.clampAgentFieldIndex()
			m.message = "agent panel opened"
			return m, nil
		case "up", "k":
			if len(m.registry.Agents) > 0 && m.selected > 0 {
				m.selected--
			}
			if strings.TrimSpace(m.createParentDir) == "" {
				m.createParentDir = defaultManagerCreateParentDir(m.registry, m.selected)
			}
			m.pickerMode = managerPickerNone
			m.pickerIndex = 0
			m.createMode = false
			m.createEditing = false
			m.createInput = ""
			m.agentMode = false
			m.agentEditing = false
			m.agentInput = ""
			return m, m.selectedRefreshCmd()
		case "down", "j":
			if len(m.registry.Agents) > 0 && m.selected < len(m.registry.Agents)-1 {
				m.selected++
			}
			if strings.TrimSpace(m.createParentDir) == "" {
				m.createParentDir = defaultManagerCreateParentDir(m.registry, m.selected)
			}
			m.pickerMode = managerPickerNone
			m.pickerIndex = 0
			m.createMode = false
			m.createEditing = false
			m.createInput = ""
			m.agentMode = false
			m.agentEditing = false
			m.agentInput = ""
			return m, m.selectedRefreshCmd()
		case "r":
			return m, func() tea.Msg {
				var live *managerLiveRuntimeStatus
				var catalog *managerWorkspaceCatalog
				if record, ok := m.selectedRecord(); ok {
					currentLive := loadManagerLiveRuntimeStatus(m.ctx, record)
					currentCatalog := loadManagerWorkspaceCatalog(m.ctx, record)
					live = &currentLive
					catalog = &currentCatalog
				}
				return managerActionMsg{text: "refreshed registry", refresh: true, live: live, catalog: catalog}
			}
		case "l":
			if m.showLogs {
				m.showLogs = false
				m.message = "logs pane closed"
				return m, nil
			}
			m.showLogs = true
			m.message = "logs pane opened"
			return m, tea.Batch(m.loadLogsForSelected(managerLogTailLines), managerLogRefreshCmd())
		case "s":
			return m, m.runStartSelected()
		case "x":
			return m, m.runStopSelected()
		case "R":
			return m, m.runRestartSelected()
		case "t":
			return m, m.runStatusSelected()
		case "p":
			return m, m.togglePauseSelected()
		case "f":
			m.message = "task filter: " + m.cycleTaskStatusFilter()
			return m, nil
		case "y":
			m.message = "tension type: " + m.cycleTensionTypeFilter()
			return m, nil
		case "v":
			m.message = "tension action: " + m.cycleTensionAction()
			return m, nil
		case "u":
			m.message = "tension lifecycle: " + m.cycleTensionLifecycle()
			return m, nil
		case "w":
			if len(m.visibleTaskChoices()) == 0 {
				return m, func() tea.Msg { return managerActionMsg{text: "no switchable tasks in catalog"} }
			}
			m.pickerMode = managerPickerTask
			m.pickerIndex = 0
			return m, nil
		case "g":
			if len(m.visibleTensionChoices()) == 0 {
				return m, func() tea.Msg { return managerActionMsg{text: "no tensions in frontier"} }
			}
			m.pickerMode = managerPickerTension
			m.pickerIndex = 0
			return m, nil
		case "c":
			return m, m.queueChatSelected()
		case "a":
			return m, m.queueAttachSelected()
		}
	}

	return m, nil
}

func (m managerVisorModel) View() string {
	if m.width <= 0 {
		return "loading..."
	}

	leftWidth := m.width / 3
	if leftWidth < 30 {
		leftWidth = 30
	}
	rightWidth := m.width - leftWidth - 3
	if rightWidth < 40 {
		rightWidth = 40
	}

	var left strings.Builder
	left.WriteString(fmt.Sprintf("%s\n", appCommandName))
	left.WriteString(fmt.Sprintf("defaults: %s @ %s\n", firstNonEmpty(m.registry.Defaults.WorkspaceID, "-"), firstNonEmpty(m.registry.Defaults.HostURL, "-")))
	left.WriteString("\nagents:\n")
	if len(m.registry.Agents) == 0 {
		left.WriteString("  (none)\n")
	} else {
		for i, record := range m.registry.Agents {
			prefix := " "
			if i == m.selected {
				prefix = ">"
			}
			status := ManagedAgentRuntimeStatus(record)
			line := fmt.Sprintf("%s %s [%s]", prefix, record.AgentID, status)
			left.WriteString(truncateVisor(line, leftWidth-2))
			left.WriteString("\n")
		}
	}

	var right strings.Builder
	right.WriteString("selected:\n")
	if record, ok := m.selectedRecord(); ok {
		taskChoices := m.visibleTaskChoices()
		tensionChoices := m.visibleTensionChoices()
		status := InspectManagedAgentProcess(record)
		local := LoadLocalRuntimeProfile(record.Workdir)
		right.WriteString(fmt.Sprintf("  agent: %s\n", firstNonEmpty(local.effectiveAgentID(), record.AgentID)))
		right.WriteString(fmt.Sprintf("  display: %s\n", firstNonEmpty(local.effectiveDisplayName(), record.DisplayName, "-")))
		right.WriteString(fmt.Sprintf("  workdir: %s\n", record.Workdir))
		right.WriteString(fmt.Sprintf("  status: %s\n", status.State))
		if status.PID > 0 {
			right.WriteString(fmt.Sprintf("  pid: %d\n", status.PID))
		}
		if workspaceID := firstNonEmpty(local.effectiveWorkspaceID(), record.WorkspaceID); workspaceID != "" {
			right.WriteString(fmt.Sprintf("  workspace: %s\n", workspaceID))
		}
		if record.HostURL != "" {
			right.WriteString(fmt.Sprintf("  host: %s\n", record.HostURL))
		}
		if role := firstNonEmpty(local.effectiveRole(), record.Role); role != "" {
			right.WriteString(fmt.Sprintf("  role: %s\n", role))
		}
		right.WriteString("\nlive runtime:\n")
		right.WriteString(renderLiveRuntimeStatus(m.liveStatus))
		right.WriteString("\nworkspace catalog:\n")
		right.WriteString(renderWorkspaceCatalog(
			m.catalog,
			taskChoices,
			tensionChoices,
			m.pickerMode,
			m.pickerIndex,
			rightWidth-2,
			managerFilterLabel(m.taskStatusFilter),
			managerFilterLabel(m.tensionTypeFilter),
			m.currentTensionActionLabel(),
		))
		right.WriteString("\nnew agent:\n")
		right.WriteString(renderManagerCreatePanel(m.createMode, m.createEditing, m.createFieldIndex, m.createInput, m.createParentDir, m.createFolderName, rightWidth-2))
		right.WriteString("\nagent registry:\n")
		right.WriteString(renderManagerAgentEditor(record, m.agentMode, m.agentEditing, m.agentFieldIndex, m.agentInput, rightWidth-2))
		right.WriteString("\ndefaults:\n")
		right.WriteString(renderManagerDefaults(
			m.registry.Defaults,
			m.defaultsMode,
			m.defaultsEditing,
			m.defaultsIndex,
			m.defaultsInput,
			rightWidth-2,
		))
		if m.showLogs {
			right.WriteString("\nlive logs:\n")
			if m.logTitle != "" {
				right.WriteString(fmt.Sprintf("  %s\n", m.logTitle))
			}
			if len(m.logLines) == 0 {
				right.WriteString("  (no log lines)\n")
			} else {
				for _, line := range m.logLines {
					right.WriteString("  " + truncateVisor(line, rightWidth-4) + "\n")
				}
			}
		}
	} else {
		right.WriteString("  no agent selected\n")
	}

	footer := m.footer()
	body := joinColumns(left.String(), right.String(), leftWidth, rightWidth)
	return body + "\n" + footer + "\n"
}

func (m managerVisorModel) footer() string {
	if m.commandMode {
		return fmt.Sprintf(":%s", m.command)
	}
	if m.createMode {
		if m.createEditing {
			spec, _, _ := m.selectedCreateField()
			return fmt.Sprintf("new agent edit | %s=%s | enter apply  esc cancel  ctrl+u clear", spec.Key, m.createInput)
		}
		return "new agent panel | j/k navigate  enter edit/launch  esc close"
	}
	if m.agentMode {
		if m.agentEditing {
			spec, _, _ := m.selectedAgentField()
			return fmt.Sprintf("agent edit | %s=%s | enter save  esc cancel  ctrl+u clear", spec.Key, m.agentInput)
		}
		return "agent panel | j/k navigate  enter edit/action  esc close"
	}
	if m.defaultsMode {
		if m.defaultsEditing {
			spec, _, _ := m.selectedDefaultField()
			return fmt.Sprintf("defaults edit | %s=%s | enter save  esc cancel  ctrl+u clear", spec.Key, m.defaultsInput)
		}
		return "defaults panel | j/k navigate  enter edit  esc close"
	}
	if m.pickerMode != managerPickerNone {
		return fmt.Sprintf("%s picker | j/k navigate  enter confirm  esc cancel  f task-filter  y tension-type  v action  u lifecycle", string(m.pickerMode))
	}
	if m.message != "" {
		return fmt.Sprintf("%s | %s", m.message, "q quit  : command  n new  o onboard  j/k navigate  s start  x stop  R restart  t live-status  p pause/resume  f task-filter  y tension-type  v action  u lifecycle  w task  g tension  e agent  l logs-pane  d defaults  c chat  a attach")
	}
	return "q quit  : command  n new  o onboard  j/k navigate  s start  x stop  R restart  t live-status  p pause/resume  f task-filter  y tension-type  v action  u lifecycle  w task  g tension  e agent  l logs-pane  d defaults  c chat  a attach"
}

func (m managerVisorModel) selectedRecord() (ManagedAgentRecord, bool) {
	if len(m.registry.Agents) == 0 {
		return ManagedAgentRecord{}, false
	}
	if m.selected < 0 || m.selected >= len(m.registry.Agents) {
		return ManagedAgentRecord{}, false
	}
	return m.registry.Agents[m.selected], true
}

func (m managerVisorModel) executeCommand(cmd string) tea.Cmd {
	fields, err := splitManagerCommand(cmd)
	if err != nil {
		return func() tea.Msg { return managerActionMsg{text: err.Error()} }
	}
	if len(fields) == 0 {
		return nil
	}
	switch strings.ToLower(fields[0]) {
	case "refresh", "list", "reload":
		return func() tea.Msg {
			var live *managerLiveRuntimeStatus
			var catalog *managerWorkspaceCatalog
			if record, ok := m.selectedRecord(); ok {
				currentLive := loadManagerLiveRuntimeStatus(m.ctx, record)
				currentCatalog := loadManagerWorkspaceCatalog(m.ctx, record)
				live = &currentLive
				catalog = &currentCatalog
			}
			return managerActionMsg{text: "refreshed registry", refresh: true, live: live, catalog: catalog}
		}
	case "defaults":
		return func() tea.Msg {
			registry := LoadBotRegistry()
			var live *managerLiveRuntimeStatus
			var catalog *managerWorkspaceCatalog
			if record, ok := m.selectedRecord(); ok {
				currentLive := loadManagerLiveRuntimeStatus(m.ctx, record)
				currentCatalog := loadManagerWorkspaceCatalog(m.ctx, record)
				live = &currentLive
				catalog = &currentCatalog
			}
			return managerActionMsg{text: formatDefaultsMessage(registry), refresh: true, live: live, catalog: catalog}
		}
	case "onboard":
		if len(fields) < 2 {
			return func() tea.Msg { return managerActionMsg{text: "usage: onboard <workdir>"} }
		}
		workdir := strings.Join(fields[1:], " ")
		return func() tea.Msg {
			exe, err := os.Executable()
			if err != nil {
				return managerActionMsg{text: err.Error()}
			}
			cmd := exec.Command(exe, "onboard", "--workdir", workdir)
			cmd.Stdin = os.Stdin
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
			return tea.ExecProcess(cmd, func(execErr error) tea.Msg {
				if execErr != nil {
					return managerActionMsg{text: execErr.Error()}
				}
				return buildManagerOnboardActionMsg(m.ctx, workdir)
			})
		}
	case "set-default":
		if len(fields) < 3 {
			return func() tea.Msg { return managerActionMsg{text: "usage: set-default <field> <value>"} }
		}
		field := fields[1]
		value := strings.Join(fields[2:], " ")
		return func() tea.Msg {
			var live *managerLiveRuntimeStatus
			var catalog *managerWorkspaceCatalog
			if record, ok := m.selectedRecord(); ok {
				currentLive := loadManagerLiveRuntimeStatus(m.ctx, record)
				currentCatalog := loadManagerWorkspaceCatalog(m.ctx, record)
				live = &currentLive
				catalog = &currentCatalog
			}
			if err := SetManagerDefault(field, value); err != nil {
				return managerActionMsg{text: err.Error(), live: live, catalog: catalog}
			}
			return managerActionMsg{text: "updated default", refresh: true, live: live, catalog: catalog}
		}
	case "clear-default":
		if len(fields) < 2 {
			return func() tea.Msg { return managerActionMsg{text: "usage: clear-default <field>"} }
		}
		field := fields[1]
		return func() tea.Msg {
			var live *managerLiveRuntimeStatus
			var catalog *managerWorkspaceCatalog
			if record, ok := m.selectedRecord(); ok {
				currentLive := loadManagerLiveRuntimeStatus(m.ctx, record)
				currentCatalog := loadManagerWorkspaceCatalog(m.ctx, record)
				live = &currentLive
				catalog = &currentCatalog
			}
			if err := ClearManagerDefault(field); err != nil {
				return managerActionMsg{text: err.Error(), live: live, catalog: catalog}
			}
			return managerActionMsg{text: "cleared default", refresh: true, live: live, catalog: catalog}
		}
	case "install":
		return func() tea.Msg {
			var out bytes.Buffer
			err := runInstallWithWriter(fields[1:], &out)
			if err != nil {
				return managerActionMsg{text: err.Error()}
			}
			return managerActionMsg{text: strings.TrimSpace(out.String())}
		}
	case "status":
		return m.runStatusSelected()
	case "logs":
		lines := 40
		if len(fields) > 1 {
			if parsed, err := strconv.Atoi(fields[len(fields)-1]); err == nil && parsed > 0 {
				lines = parsed
			}
		}
		return m.loadLogsForSelected(lines)
	case "start":
		return m.runStartSelectedWithOptions(managerTUIManagedRunPreflightOptions(fields[1:]))
	case "stop":
		return m.runStopSelected()
	case "restart":
		return m.runRestartSelectedWithOptions(managerTUIManagedRunPreflightOptions(fields[1:]))
	case "chat":
		return m.queueChatSelected()
	case "attach":
		if len(fields) > 1 {
			record, err := ResolveManagedAgentReference(strings.Join(fields[1:], " "))
			if err != nil {
				return func() tea.Msg { return managerActionMsg{text: err.Error()} }
			}
			return func() tea.Msg {
				live := loadManagerLiveRuntimeStatus(m.ctx, record)
				catalog := loadManagerWorkspaceCatalog(m.ctx, record)
				return managerActionMsg{
					text:    fmt.Sprintf("opening live attach for %s", record.AgentID),
					attach:  record.AgentID,
					live:    &live,
					catalog: &catalog,
				}
			}
		}
		return m.queueAttachSelected()
	case "pause":
		return m.runControlSelected("runtime.pause", map[string]any{
			"reason": firstNonEmpty(strings.Join(fields[1:], " "), "operator pause"),
		})
	case "resume":
		return m.runControlSelected("runtime.resume", map[string]any{
			"reason": firstNonEmpty(strings.Join(fields[1:], " "), "operator resume"),
		})
	case "switch-task":
		if len(fields) < 2 {
			return func() tea.Msg { return managerActionMsg{text: "usage: switch-task <task_id> [session_id] [reason]"} }
		}
		payload := map[string]any{
			"task_id": fields[1],
			"reason":  "operator switch task",
		}
		reasonStart := 2
		if len(fields) > 2 {
			payload["session_id"] = fields[2]
			reasonStart = 3
		}
		if len(fields) > reasonStart {
			payload["reason"] = strings.Join(fields[reasonStart:], " ")
		}
		return m.runControlSelected("runtime.switch_task", payload)
	case "switch-tension":
		if len(fields) < 2 {
			return func() tea.Msg {
				return managerActionMsg{text: "usage: switch-tension <tension_id> [attach|detach|lifecycle] [role] [state] [reason]"}
			}
		}
		payload := map[string]any{
			"tension_id": fields[1],
			"reason":     "operator switch tension",
		}
		idx := 2
		if len(fields) > 2 {
			switch strings.ToLower(fields[2]) {
			case "attach", "focus", "detach", "release", "lifecycle", "update":
				payload["action"] = fields[2]
				idx = 3
			}
		}
		action := strings.ToLower(mapStringValue(payload, "action"))
		if action == "" {
			action = "attach"
		}
		switch action {
		case "attach", "focus":
			if len(fields) > idx {
				payload["role"] = fields[idx]
				idx++
			}
			if len(fields) > idx {
				payload["lifecycle_state"] = fields[idx]
				idx++
			}
		case "lifecycle", "update":
			if len(fields) > idx {
				payload["lifecycle_state"] = fields[idx]
				idx++
			}
		}
		if len(fields) > idx {
			payload["reason"] = strings.Join(fields[idx:], " ")
		}
		return m.runControlSelected("runtime.switch_tension", payload)
	default:
		return func() tea.Msg {
			return managerActionMsg{text: fmt.Sprintf("unknown command %q", fields[0])}
		}
	}
}

func (m managerVisorModel) runStartSelected() tea.Cmd {
	return m.runStartSelectedWithOptions(managedRunPreflightOptions{})
}

func (m managerVisorModel) runStartSelectedWithOptions(preflightOptions managedRunPreflightOptions) tea.Cmd {
	record, ok := m.selectedRecord()
	if !ok {
		return func() tea.Msg { return managerActionMsg{text: "no agent selected"} }
	}
	return func() tea.Msg {
		state, err := StartManagedAgentWithOptions(record, preflightOptions)
		if err != nil {
			live := loadManagerLiveRuntimeStatus(m.ctx, record)
			catalog := loadManagerWorkspaceCatalog(m.ctx, record)
			return managerActionMsg{
				text:    formatManagedProcessActionError(record, err),
				refresh: true,
				live:    &live,
				catalog: &catalog,
			}
		}
		live := loadManagerLiveRuntimeStatus(m.ctx, record)
		catalog := loadManagerWorkspaceCatalog(m.ctx, record)
		return managerActionMsg{
			text:    fmt.Sprintf("started %s pid=%d", record.AgentID, state.PID),
			refresh: true,
			live:    &live,
			catalog: &catalog,
		}
	}
}

func (m managerVisorModel) runStopSelected() tea.Cmd {
	record, ok := m.selectedRecord()
	if !ok {
		return func() tea.Msg { return managerActionMsg{text: "no agent selected"} }
	}
	return func() tea.Msg {
		if err := StopManagedAgent(record); err != nil {
			live := loadManagerLiveRuntimeStatus(m.ctx, record)
			catalog := loadManagerWorkspaceCatalog(m.ctx, record)
			return managerActionMsg{
				text:    formatManagedProcessActionError(record, err),
				refresh: true,
				live:    &live,
				catalog: &catalog,
			}
		}
		live := loadManagerLiveRuntimeStatus(m.ctx, record)
		catalog := loadManagerWorkspaceCatalog(m.ctx, record)
		return managerActionMsg{
			text:    fmt.Sprintf("stopped %s", record.AgentID),
			refresh: true,
			live:    &live,
			catalog: &catalog,
		}
	}
}

func (m managerVisorModel) runRestartSelected() tea.Cmd {
	return m.runRestartSelectedWithOptions(managedRunPreflightOptions{})
}

func (m managerVisorModel) runRestartSelectedWithOptions(preflightOptions managedRunPreflightOptions) tea.Cmd {
	record, ok := m.selectedRecord()
	if !ok {
		return func() tea.Msg { return managerActionMsg{text: "no agent selected"} }
	}
	return func() tea.Msg {
		state, err := RestartManagedAgentWithOptions(record, preflightOptions)
		if err != nil {
			live := loadManagerLiveRuntimeStatus(m.ctx, record)
			catalog := loadManagerWorkspaceCatalog(m.ctx, record)
			return managerActionMsg{
				text:    formatManagedProcessActionError(record, err),
				refresh: true,
				live:    &live,
				catalog: &catalog,
			}
		}
		live := loadManagerLiveRuntimeStatus(m.ctx, record)
		catalog := loadManagerWorkspaceCatalog(m.ctx, record)
		return managerActionMsg{
			text:    fmt.Sprintf("restarted %s pid=%d", record.AgentID, state.PID),
			refresh: true,
			live:    &live,
			catalog: &catalog,
		}
	}
}

func managerTUIManagedRunPreflightOptions(fields []string) managedRunPreflightOptions {
	var options managedRunPreflightOptions
	for _, field := range fields {
		token := strings.ToLower(strings.TrimSpace(strings.TrimLeft(field, "-")))
		switch token {
		case "resume", "resume-continuation", "allow-resume":
			options.ResumeContinuationWaiver = managedRunFullResumeContinuationWaiver()
		case "dirty-project-checkout", "allow-dirty-project-checkout", "resume-dirty-project-checkout":
			options.ResumeContinuationWaiver.AllowDirtyProjectCheckout = true
		case "live-patch-queue", "allow-live-patch-queue", "resume-live-patch-queue":
			options.ResumeContinuationWaiver.AllowLivePatchQueue = true
		case "agent-request", "agent-requests", "allow-agent-requests", "resume-agent-requests":
			options.ResumeContinuationWaiver.AllowAgentRequests = true
		case "live-project-branch", "live-project-branches", "allow-live-project-branches", "resume-live-project-branches":
			options.ResumeContinuationWaiver.AllowLiveProjectBranches = true
		case "pending-resume-trigger", "pending-resume-triggers", "allow-pending-resume-triggers", "resume-pending-triggers":
			options.ResumeContinuationWaiver.AllowPendingResumeTriggers = true
		}
	}
	return options
}

func formatManagedProcessActionError(record ManagedAgentRecord, err error) string {
	msg := strings.TrimSpace(fmt.Sprint(err))
	process := InspectManagedAgentProcess(record)
	processSummary := "process: " + firstNonEmpty(strings.TrimSpace(process.State), "unknown")
	if process.PID > 0 {
		processSummary += fmt.Sprintf(" pid=%d", process.PID)
	}
	if msg == "" {
		return processSummary
	}
	return msg + " (" + processSummary + ")"
}

func buildManagedAgentStateErrorMsg(ctx context.Context, record ManagedAgentRecord, err error) managerActionMsg {
	live := loadManagerLiveRuntimeStatus(ctx, record)
	catalog := loadManagerWorkspaceCatalog(ctx, record)
	return managerActionMsg{
		text:    formatManagedProcessActionError(record, err),
		refresh: true,
		live:    &live,
		catalog: &catalog,
	}
}

func formatManagedControlActionError(method string, result managedAgentControlRequestResult, err error) string {
	msg := strings.TrimSpace(fmt.Sprint(err))
	if !hasManagedAgentControlRequestResult(result) {
		if msg != "" {
			return msg
		}
		return firstNonEmpty(strings.TrimSpace(method), "control request failed")
	}

	details := make([]string, 0, 3)
	if requestID := strings.TrimSpace(result.RequestID); requestID != "" {
		details = append(details, "request_id="+requestID)
	}
	if status := strings.TrimSpace(result.Status); status != "" {
		details = append(details, "status="+status)
	}
	if response := firstNonEmpty(prettyJSONText(strings.TrimSpace(result.Response)), strings.TrimSpace(result.Response)); response != "" {
		response = strings.Join(strings.Fields(response), " ")
		details = append(details, "response="+truncateVisor(response, 160))
	}
	if msg == "" {
		msg = firstNonEmpty(strings.TrimSpace(method), "control request failed")
	}
	return msg + " | " + strings.Join(details, " ")
}

func (m managerVisorModel) runStatusSelected() tea.Cmd {
	record, ok := m.selectedRecord()
	if !ok {
		return func() tea.Msg { return managerActionMsg{text: "no agent selected"} }
	}
	return func() tea.Msg {
		status := InspectManagedAgentProcess(record)
		msg := fmt.Sprintf("%s process: %s", record.AgentID, status.State)
		if status.PID > 0 {
			msg += fmt.Sprintf(" pid=%d", status.PID)
		}
		live := loadManagerLiveRuntimeStatus(m.ctx, record)
		catalog := loadManagerWorkspaceCatalog(m.ctx, record)
		return managerActionMsg{text: msg, refresh: true, live: &live, catalog: &catalog}
	}
}

func (m managerVisorModel) loadLogsForSelected(lines int) tea.Cmd {
	record, ok := m.selectedRecord()
	if !ok {
		return func() tea.Msg { return managerActionMsg{text: "no agent selected"} }
	}
	return func() tea.Msg {
		tail, err := TailManagedAgentLogs(record, lines)
		if err != nil {
			live := loadManagerLiveRuntimeStatus(m.ctx, record)
			catalog := loadManagerWorkspaceCatalog(m.ctx, record)
			return managerActionMsg{
				text:    fmt.Sprintf("load logs for %s: %v", record.AgentID, err),
				refresh: false,
				live:    &live,
				catalog: &catalog,
			}
		}
		merged := append([]string(nil), tail.Stdout...)
		if len(tail.Stderr) > 0 {
			merged = append(merged, "---- stderr ----")
			merged = append(merged, tail.Stderr...)
		}
		live := loadManagerLiveRuntimeStatus(m.ctx, record)
		catalog := loadManagerWorkspaceCatalog(m.ctx, record)
		return managerActionMsg{
			text:     fmt.Sprintf("loaded logs for %s", record.AgentID),
			loadLogs: true,
			logTail:  merged,
			logTitle: fmt.Sprintf("stdout: %s | stderr: %s", tail.LogOutPath, tail.LogErrPath),
			live:     &live,
			catalog:  &catalog,
		}
	}
}

func managerLogRefreshCmd() tea.Cmd {
	return tea.Tick(managerLogRefreshInterval, func(time.Time) tea.Msg {
		return managerLogTickMsg{}
	})
}

func (m managerVisorModel) queueChatSelected() tea.Cmd {
	record, ok := m.selectedRecord()
	if !ok {
		return func() tea.Msg { return managerActionMsg{text: "no agent selected"} }
	}
	return func() tea.Msg {
		live := loadManagerLiveRuntimeStatus(m.ctx, record)
		catalog := loadManagerWorkspaceCatalog(m.ctx, record)
		return managerActionMsg{
			text:      fmt.Sprintf("opening chat for %s", record.AgentID),
			chatAgent: record.AgentID,
			live:      &live,
			catalog:   &catalog,
		}
	}
}

func (m managerVisorModel) queueAttachSelected() tea.Cmd {
	record, ok := m.selectedRecord()
	if !ok {
		return func() tea.Msg { return managerActionMsg{text: "no agent selected"} }
	}
	return func() tea.Msg {
		live := loadManagerLiveRuntimeStatus(m.ctx, record)
		catalog := loadManagerWorkspaceCatalog(m.ctx, record)
		return managerActionMsg{
			text:    fmt.Sprintf("opening live attach for %s", record.AgentID),
			attach:  record.AgentID,
			live:    &live,
			catalog: &catalog,
		}
	}
}

func (m managerVisorModel) runControlSelected(method string, payload map[string]any) tea.Cmd {
	record, ok := m.selectedRecord()
	if !ok {
		return func() tea.Msg { return managerActionMsg{text: "no agent selected"} }
	}
	return func() tea.Msg {
		result, err := sendManagedAgentControlRequest(m.ctx, record, method, payload)
		if err != nil {
			live := loadManagerLiveRuntimeStatus(m.ctx, record)
			catalog := loadManagerWorkspaceCatalog(m.ctx, record)
			return managerActionMsg{
				text:    formatManagedControlActionError(method, result, err),
				refresh: true,
				live:    &live,
				catalog: &catalog,
			}
		}
		msg := fmt.Sprintf("%s -> %s [%s]", record.AgentID, method, firstNonEmpty(result.Status, "submitted"))
		if strings.TrimSpace(result.Response) != "" {
			msg += " | " + truncateVisor(result.Response, 120)
		}
		live := loadManagerLiveRuntimeStatus(m.ctx, record)
		catalog := loadManagerWorkspaceCatalog(m.ctx, record)
		return managerActionMsg{text: msg, refresh: true, live: &live, catalog: &catalog}
	}
}

func (m managerVisorModel) loadLiveStatusForSelected() tea.Cmd {
	record, ok := m.selectedRecord()
	if !ok {
		return func() tea.Msg {
			return managerActionMsg{live: &managerLiveRuntimeStatus{}}
		}
	}
	return func() tea.Msg {
		live := loadManagerLiveRuntimeStatus(m.ctx, record)
		return managerActionMsg{live: &live}
	}
}

func (m managerVisorModel) loadCatalogForSelected() tea.Cmd {
	record, ok := m.selectedRecord()
	if !ok {
		return func() tea.Msg {
			return managerActionMsg{catalog: &managerWorkspaceCatalog{}}
		}
	}
	return func() tea.Msg {
		catalog := loadManagerWorkspaceCatalog(m.ctx, record)
		return managerActionMsg{catalog: &catalog}
	}
}

func (m managerVisorModel) selectedRefreshCmd() tea.Cmd {
	cmds := []tea.Cmd{
		m.loadLiveStatusForSelected(),
		m.loadCatalogForSelected(),
	}
	if m.showLogs {
		cmds = append(cmds, m.loadLogsForSelected(managerLogTailLines))
	}
	return tea.Batch(cmds...)
}

func (m managerVisorModel) togglePauseSelected() tea.Cmd {
	if _, ok := m.selectedRecord(); !ok {
		return func() tea.Msg { return managerActionMsg{text: "no agent selected"} }
	}
	if m.liveStatus != nil && strings.EqualFold(m.liveStatus.ProcessState, "running") && m.liveStatus.Paused {
		return m.runControlSelected("runtime.resume", map[string]any{"reason": "visor resume"})
	}
	return m.runControlSelected("runtime.pause", map[string]any{"reason": "visor pause"})
}

func (m *managerVisorModel) clampCreateFieldIndex() {
	if m == nil {
		return
	}
	if len(managerCreateFieldSpecs) == 0 {
		m.createFieldIndex = 0
		return
	}
	if m.createFieldIndex < 0 {
		m.createFieldIndex = 0
	}
	if m.createFieldIndex >= len(managerCreateFieldSpecs) {
		m.createFieldIndex = len(managerCreateFieldSpecs) - 1
	}
}

func (m managerVisorModel) selectedCreateField() (managerCreateFieldSpec, string, bool) {
	if len(managerCreateFieldSpecs) == 0 {
		return managerCreateFieldSpec{}, "", false
	}
	index := m.createFieldIndex
	if index < 0 || index >= len(managerCreateFieldSpecs) {
		index = 0
	}
	spec := managerCreateFieldSpecs[index]
	return spec, managerCreateFieldValue(spec.Key, m.createParentDir, m.createFolderName), true
}

func (m managerVisorModel) launchNewAgentOnboard() tea.Cmd {
	workdir, err := resolveManagerCreateWorkdir(m.createParentDir, m.createFolderName)
	if err != nil {
		return func() tea.Msg { return managerActionMsg{text: err.Error()} }
	}
	return func() tea.Msg {
		if err := os.MkdirAll(workdir, 0o755); err != nil {
			return managerActionMsg{text: fmt.Sprintf("create workdir: %v", err)}
		}
		exe, err := os.Executable()
		if err != nil {
			return managerActionMsg{text: err.Error()}
		}
		cmd := exec.Command(exe, "onboard", "--workdir", workdir)
		cmd.Stdin = os.Stdin
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		return tea.ExecProcess(cmd, func(execErr error) tea.Msg {
			if execErr != nil {
				return managerActionMsg{text: execErr.Error()}
			}
			return buildManagerOnboardActionMsg(m.ctx, workdir)
		})
	}
}

func buildManagerOnboardActionMsg(ctx context.Context, workdir string) managerActionMsg {
	msg := managerActionMsg{
		text:          fmt.Sprintf("onboarded %s", workdir),
		refresh:       true,
		selectWorkdir: workdir,
	}
	registry := LoadBotRegistry()
	if selected := findManagedAgentIndexByWorkdir(registry, workdir); selected >= 0 {
		record := registry.Agents[selected]
		live := loadManagerLiveRuntimeStatus(ctx, record)
		catalog := loadManagerWorkspaceCatalog(ctx, record)
		msg.live = &live
		msg.catalog = &catalog
	}
	return msg
}

func (m *managerVisorModel) clampAgentFieldIndex() {
	if m == nil {
		return
	}
	if len(managerAgentFieldSpecs) == 0 {
		m.agentFieldIndex = 0
		return
	}
	if m.agentFieldIndex < 0 {
		m.agentFieldIndex = 0
	}
	if m.agentFieldIndex >= len(managerAgentFieldSpecs) {
		m.agentFieldIndex = len(managerAgentFieldSpecs) - 1
	}
}

func (m managerVisorModel) selectedAgentField() (managerAgentFieldSpec, string, bool) {
	record, ok := m.selectedRecord()
	if !ok || len(managerAgentFieldSpecs) == 0 {
		return managerAgentFieldSpec{}, "", false
	}
	index := m.agentFieldIndex
	if index < 0 || index >= len(managerAgentFieldSpecs) {
		index = 0
	}
	spec := managerAgentFieldSpecs[index]
	return spec, managerAgentFieldValue(record, spec.Key), true
}

func (m managerVisorModel) saveSelectedAgentField(spec managerAgentFieldSpec, value string) tea.Cmd {
	record, ok := m.selectedRecord()
	if !ok {
		return func() tea.Msg { return managerActionMsg{text: "no agent selected"} }
	}
	return func() tea.Msg {
		status := InspectManagedAgentProcess(record)
		switch spec.Key {
		case "__remove__":
			if status.Running {
				live := loadManagerLiveRuntimeStatus(m.ctx, record)
				catalog := loadManagerWorkspaceCatalog(m.ctx, record)
				return managerActionMsg{
					text:    formatManagedProcessActionError(record, fmt.Errorf("stop %s before removing it", record.AgentID)),
					refresh: true,
					live:    &live,
					catalog: &catalog,
				}
			}
			if err := RemoveManagedAgent(record.AgentID); err != nil {
				return buildManagedAgentStateErrorMsg(m.ctx, record, err)
			}
			updatedRegistry := LoadBotRegistry()
			if len(updatedRegistry.Agents) == 0 {
				emptyLive := managerLiveRuntimeStatus{}
				emptyCatalog := managerWorkspaceCatalog{}
				return managerActionMsg{
					text:    fmt.Sprintf("removed %s from registry", record.AgentID),
					refresh: true,
					live:    &emptyLive,
					catalog: &emptyCatalog,
				}
			}
			nextSelected := m.selected
			if nextSelected < 0 {
				nextSelected = 0
			}
			if nextSelected >= len(updatedRegistry.Agents) {
				nextSelected = len(updatedRegistry.Agents) - 1
			}
			nextRecord := updatedRegistry.Agents[nextSelected]
			live := loadManagerLiveRuntimeStatus(m.ctx, nextRecord)
			catalog := loadManagerWorkspaceCatalog(m.ctx, nextRecord)
			return managerActionMsg{
				text:          fmt.Sprintf("removed %s from registry", record.AgentID),
				refresh:       true,
				selectWorkdir: nextRecord.Workdir,
				live:          &live,
				catalog:       &catalog,
			}
		case "display_name":
			if status.Running {
				live := loadManagerLiveRuntimeStatus(m.ctx, record)
				catalog := loadManagerWorkspaceCatalog(m.ctx, record)
				return managerActionMsg{
					text:    formatManagedProcessActionError(record, fmt.Errorf("stop %s before editing registry fields", record.AgentID)),
					refresh: true,
					live:    &live,
					catalog: &catalog,
				}
			}
			record.DisplayName = strings.TrimSpace(value)
		case "workdir":
			if status.Running {
				live := loadManagerLiveRuntimeStatus(m.ctx, record)
				catalog := loadManagerWorkspaceCatalog(m.ctx, record)
				return managerActionMsg{
					text:    formatManagedProcessActionError(record, fmt.Errorf("stop %s before moving its workdir", record.AgentID)),
					refresh: true,
					live:    &live,
					catalog: &catalog,
				}
			}
			newWorkdir, note, err := moveManagedAgentWorkdir(record.Workdir, value)
			if err != nil {
				return buildManagedAgentStateErrorMsg(m.ctx, record, err)
			}
			record.Workdir = newWorkdir
			if err := UpsertManagedAgent(record); err != nil {
				return buildManagedAgentStateErrorMsg(m.ctx, record, err)
			}
			live := loadManagerLiveRuntimeStatus(m.ctx, record)
			catalog := loadManagerWorkspaceCatalog(m.ctx, record)
			return managerActionMsg{
				text:    fmt.Sprintf("updated workdir for %s%s", record.AgentID, note),
				refresh: true,
				live:    &live,
				catalog: &catalog,
			}
		default:
			return managerActionMsg{text: spec.Label + " is not editable"}
		}
		if err := UpsertManagedAgent(record); err != nil {
			return buildManagedAgentStateErrorMsg(m.ctx, record, err)
		}
		live := loadManagerLiveRuntimeStatus(m.ctx, record)
		catalog := loadManagerWorkspaceCatalog(m.ctx, record)
		return managerActionMsg{
			text:    fmt.Sprintf("updated %s for %s", spec.Label, record.AgentID),
			refresh: true,
			live:    &live,
			catalog: &catalog,
		}
	}
}

func findManagedAgentIndexByWorkdir(registry BotRegistry, workdir string) int {
	target := strings.TrimSpace(workdir)
	if target == "" {
		return -1
	}
	if resolved, err := filepath.Abs(target); err == nil {
		target = resolved
	}
	target = filepath.Clean(target)
	for i, record := range registry.Agents {
		candidate := strings.TrimSpace(record.Workdir)
		if candidate == "" {
			continue
		}
		if resolved, err := filepath.Abs(candidate); err == nil {
			candidate = resolved
		}
		if strings.EqualFold(filepath.Clean(candidate), target) {
			return i
		}
	}
	return -1
}

func moveManagedAgentWorkdir(currentWorkdir, target string) (string, string, error) {
	target = strings.TrimSpace(target)
	if target == "" {
		return "", "", fmt.Errorf("workdir cannot be empty")
	}
	targetAbs, err := validateManagedAgentWorkdirTarget(target)
	if err != nil {
		return "", "", err
	}
	currentAbs := strings.TrimSpace(currentWorkdir)
	if currentAbs != "" {
		if currentResolved, err := filepath.Abs(currentAbs); err == nil {
			currentAbs = currentResolved
		}
	}
	if currentAbs != "" && strings.EqualFold(filepath.Clean(currentAbs), filepath.Clean(targetAbs)) {
		return targetAbs, "", nil
	}
	if currentAbs != "" {
		if info, err := os.Stat(currentAbs); err == nil && info.IsDir() {
			if _, err := os.Stat(targetAbs); err == nil {
				return "", "", fmt.Errorf("target workdir already exists: %s", targetAbs)
			} else if !os.IsNotExist(err) {
				return "", "", fmt.Errorf("inspect target workdir: %w", err)
			}
			if err := os.MkdirAll(filepath.Dir(targetAbs), 0o755); err != nil {
				return "", "", fmt.Errorf("prepare target workdir: %w", err)
			}
			if err := os.Rename(currentAbs, targetAbs); err != nil {
				return "", "", fmt.Errorf("move workdir: %w", err)
			}
			return targetAbs, " (moved folder)", nil
		} else if err != nil && !os.IsNotExist(err) {
			return "", "", fmt.Errorf("inspect current workdir: %w", err)
		}
	}
	if err := os.MkdirAll(targetAbs, 0o755); err != nil {
		return "", "", fmt.Errorf("create target workdir: %w", err)
	}
	return targetAbs, " (created target folder)", nil
}

func (m *managerVisorModel) clampDefaultsIndex() {
	if m == nil {
		return
	}
	if len(managerDefaultFieldSpecs) == 0 {
		m.defaultsIndex = 0
		return
	}
	if m.defaultsIndex < 0 {
		m.defaultsIndex = 0
	}
	if m.defaultsIndex >= len(managerDefaultFieldSpecs) {
		m.defaultsIndex = len(managerDefaultFieldSpecs) - 1
	}
}

func (m managerVisorModel) selectedDefaultField() (managerDefaultFieldSpec, string, bool) {
	if len(managerDefaultFieldSpecs) == 0 {
		return managerDefaultFieldSpec{}, "", false
	}
	index := m.defaultsIndex
	if index < 0 || index >= len(managerDefaultFieldSpecs) {
		index = 0
	}
	spec := managerDefaultFieldSpecs[index]
	return spec, managerDefaultFieldValue(m.registry.Defaults, spec.Key), true
}

func (m managerVisorModel) saveDefaultField(field, value string) tea.Cmd {
	return func() tea.Msg {
		var live *managerLiveRuntimeStatus
		var catalog *managerWorkspaceCatalog
		if record, ok := m.selectedRecord(); ok {
			currentLive := loadManagerLiveRuntimeStatus(m.ctx, record)
			currentCatalog := loadManagerWorkspaceCatalog(m.ctx, record)
			live = &currentLive
			catalog = &currentCatalog
		}
		if err := SetManagerDefault(field, value); err != nil {
			return managerActionMsg{text: err.Error(), live: live, catalog: catalog}
		}
		label := field
		for _, spec := range managerDefaultFieldSpecs {
			if spec.Key == field {
				label = spec.Label
				break
			}
		}
		if strings.TrimSpace(value) == "" {
			return managerActionMsg{text: fmt.Sprintf("reset default %s", label), refresh: true, live: live, catalog: catalog}
		}
		return managerActionMsg{text: fmt.Sprintf("updated default %s", label), refresh: true, live: live, catalog: catalog}
	}
}

func (m managerVisorModel) visibleTaskChoices() []WorkspaceTaskRecord {
	if m.catalog == nil || len(m.catalog.Tasks) == 0 {
		return nil
	}
	filter := strings.ToUpper(strings.TrimSpace(m.taskStatusFilter))
	if filter == "" {
		return append([]WorkspaceTaskRecord(nil), m.catalog.Tasks...)
	}
	choices := make([]WorkspaceTaskRecord, 0, len(m.catalog.Tasks))
	for _, task := range m.catalog.Tasks {
		if strings.EqualFold(strings.TrimSpace(task.Status), filter) {
			choices = append(choices, task)
		}
	}
	return choices
}

func (m managerVisorModel) visibleTensionChoices() []TensionFrontierItem {
	if m.catalog == nil || len(m.catalog.Tensions) == 0 {
		return nil
	}
	filter := strings.ToUpper(strings.TrimSpace(m.tensionTypeFilter))
	if filter == "" {
		return append([]TensionFrontierItem(nil), m.catalog.Tensions...)
	}
	choices := make([]TensionFrontierItem, 0, len(m.catalog.Tensions))
	for _, tension := range m.catalog.Tensions {
		if strings.EqualFold(strings.TrimSpace(tension.TensionType), filter) {
			choices = append(choices, tension)
		}
	}
	return choices
}

func (m managerVisorModel) taskStatusOptions() []string {
	options := []string{""}
	if m.catalog == nil {
		return options
	}
	seen := map[string]struct{}{}
	for _, task := range m.catalog.Tasks {
		status := strings.ToUpper(strings.TrimSpace(task.Status))
		if status == "" {
			continue
		}
		if _, ok := seen[status]; ok {
			continue
		}
		seen[status] = struct{}{}
		options = append(options, status)
	}
	return options
}

func (m managerVisorModel) tensionTypeOptions() []string {
	options := []string{""}
	if m.catalog == nil {
		return options
	}
	seen := map[string]struct{}{}
	for _, tension := range m.catalog.Tensions {
		tensionType := strings.ToUpper(strings.TrimSpace(tension.TensionType))
		if tensionType == "" {
			continue
		}
		if _, ok := seen[tensionType]; ok {
			continue
		}
		seen[tensionType] = struct{}{}
		options = append(options, tensionType)
	}
	return options
}

func (m *managerVisorModel) cycleTaskStatusFilter() string {
	if m == nil {
		return "all"
	}
	m.taskStatusFilter = cycleManagerOption(m.taskStatusFilter, m.taskStatusOptions())
	m.clampPickerIndex()
	return managerFilterLabel(m.taskStatusFilter)
}

func (m *managerVisorModel) cycleTensionTypeFilter() string {
	if m == nil {
		return "all"
	}
	m.tensionTypeFilter = cycleManagerOption(m.tensionTypeFilter, m.tensionTypeOptions())
	m.clampPickerIndex()
	return managerFilterLabel(m.tensionTypeFilter)
}

func (m *managerVisorModel) cycleTensionAction() string {
	if m == nil {
		return "focus"
	}
	m.tensionAction = cycleManagerOption(firstNonEmpty(m.tensionAction, "focus"), managerTensionActions)
	return m.currentTensionActionLabel()
}

func (m *managerVisorModel) cycleTensionLifecycle() string {
	if m == nil {
		return "ACTIVE"
	}
	m.tensionLifecycle = cycleManagerOption(firstNonEmpty(m.tensionLifecycle, "ACTIVE"), managerTensionLifecycleStates)
	return firstNonEmpty(strings.TrimSpace(m.tensionLifecycle), "ACTIVE")
}

func (m managerVisorModel) currentTensionActionLabel() string {
	action := firstNonEmpty(strings.TrimSpace(m.tensionAction), "focus")
	if strings.EqualFold(action, "lifecycle") {
		return action + ":" + firstNonEmpty(strings.TrimSpace(m.tensionLifecycle), "ACTIVE")
	}
	return action
}

func (m managerVisorModel) currentPickerLen() int {
	switch m.pickerMode {
	case managerPickerTask:
		return len(m.visibleTaskChoices())
	case managerPickerTension:
		return len(m.visibleTensionChoices())
	default:
		return 0
	}
}

func (m *managerVisorModel) clampPickerIndex() {
	if m == nil {
		return
	}
	max := m.currentPickerLen()
	if max <= 0 {
		m.pickerMode = managerPickerNone
		m.pickerIndex = 0
		return
	}
	if m.pickerIndex >= max {
		m.pickerIndex = max - 1
	}
	if m.pickerIndex < 0 {
		m.pickerIndex = 0
	}
}

func (m managerVisorModel) confirmPickerSelection() tea.Cmd {
	if _, ok := m.selectedRecord(); !ok {
		return func() tea.Msg { return managerActionMsg{text: "no agent selected"} }
	}
	switch m.pickerMode {
	case managerPickerTask:
		choices := m.visibleTaskChoices()
		if m.catalog == nil || m.pickerIndex < 0 || m.pickerIndex >= len(choices) {
			return func() tea.Msg { return managerActionMsg{text: "no task selected"} }
		}
		task := choices[m.pickerIndex]
		m.pickerMode = managerPickerNone
		return m.runControlSelected("runtime.switch_task", map[string]any{
			"task_id": task.TaskID,
			"reason":  "visor task picker",
		})
	case managerPickerTension:
		choices := m.visibleTensionChoices()
		if m.catalog == nil || m.pickerIndex < 0 || m.pickerIndex >= len(choices) {
			return func() tea.Msg { return managerActionMsg{text: "no tension selected"} }
		}
		tension := choices[m.pickerIndex]
		m.pickerMode = managerPickerNone
		payload := map[string]any{
			"tension_id": tension.TensionID,
			"reason":     "visor tension picker",
		}
		action := firstNonEmpty(strings.TrimSpace(m.tensionAction), "focus")
		payload["action"] = action
		if strings.EqualFold(action, "lifecycle") {
			payload["lifecycle_state"] = firstNonEmpty(strings.TrimSpace(m.tensionLifecycle), "ACTIVE")
		}
		return m.runControlSelected("runtime.switch_tension", payload)
	default:
		return nil
	}
}

func runManagerVisor(ctx context.Context) error {
	model := newManagerVisorModel(ctx)
	prog := tea.NewProgram(model, tea.WithAltScreen())
	finalModel, err := prog.Run()
	if err != nil {
		return err
	}
	if visor, ok := finalModel.(managerVisorModel); ok {
		if visor.pendingAttachAgent() != "" {
			return runAttachAgent([]string{visor.pendingAttachAgent()})
		}
		if visor.pendingChatAgent() != "" {
			return runChatAgent([]string{visor.pendingChatAgent()})
		}
	}
	return nil
}

func (m managerVisorModel) pendingChatAgent() string {
	return strings.TrimSpace(m.pendingChat)
}

func (m managerVisorModel) pendingAttachAgent() string {
	return strings.TrimSpace(m.pendingAttach)
}

func formatDefaultsMessage(registry BotRegistry) string {
	return fmt.Sprintf("defaults: workspace=%s host=%s backend=%s model=%s role=%s",
		firstNonEmpty(registry.Defaults.WorkspaceID, "-"),
		firstNonEmpty(registry.Defaults.HostURL, "-"),
		firstNonEmpty(registry.Defaults.LLMBackend, "-"),
		firstNonEmpty(registry.Defaults.Model, "-"),
		firstNonEmpty(registry.Defaults.Role, "-"),
	)
}

func loadManagerLiveRuntimeStatus(ctx context.Context, record ManagedAgentRecord) managerLiveRuntimeStatus {
	record = normalizeManagedAgentRecord(record)
	process := InspectManagedAgentProcess(record)
	status := managerLiveRuntimeStatus{ProcessState: process.State}
	if !process.Running {
		return status
	}
	result, err := sendManagedAgentControlRequest(ctx, record, "runtime.status", map[string]any{
		"reason": "visor refresh",
	})
	if err != nil {
		status.Error = formatManagedControlActionError("runtime.status", result, err)
		return status
	}
	return decodeManagerLiveRuntimeStatus(result.Response, process.State)
}

func loadManagerWorkspaceCatalog(ctx context.Context, record ManagedAgentRecord) managerWorkspaceCatalog {
	control, err := managedAgentControlClientForRecord(record)
	if err != nil {
		return managerWorkspaceCatalog{Error: err.Error()}
	}
	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	tasks, taskErr := control.Client.ListTasks(queryCtx, control.WorkspaceID)
	tensions, tensionErr := control.Client.GetTensionFrontier(queryCtx, control.WorkspaceID, 8)

	catalog := managerWorkspaceCatalog{
		Tasks:    selectManagerTaskChoices(tasks),
		Tensions: selectManagerTensionChoices(tensions),
	}
	if taskErr != nil && tensionErr != nil {
		catalog.Error = fmt.Sprintf("tasks: %v | tensions: %v", taskErr, tensionErr)
	} else if taskErr != nil {
		catalog.Error = "tasks: " + taskErr.Error()
	} else if tensionErr != nil {
		catalog.Error = "tensions: " + tensionErr.Error()
	}
	return catalog
}

func selectManagerTaskChoices(tasks []WorkspaceTaskRecord) []WorkspaceTaskRecord {
	if len(tasks) == 0 {
		return nil
	}
	choices := make([]WorkspaceTaskRecord, 0, 8)
	for _, task := range tasks {
		if isClosedTaskStatus(task.Status) {
			continue
		}
		choices = append(choices, task)
		if len(choices) == 8 {
			return choices
		}
	}
	if len(choices) > 0 {
		return choices
	}
	if len(tasks) > 8 {
		return append([]WorkspaceTaskRecord(nil), tasks[:8]...)
	}
	return append([]WorkspaceTaskRecord(nil), tasks...)
}

func selectManagerTensionChoices(items []TensionFrontierItem) []TensionFrontierItem {
	if len(items) > 8 {
		return append([]TensionFrontierItem(nil), items[:8]...)
	}
	return append([]TensionFrontierItem(nil), items...)
}

func isClosedTaskStatus(status string) bool {
	switch strings.ToUpper(strings.TrimSpace(status)) {
	case "DONE", "COMPLETED", "RESOLVED", "CANCELLED", "CLOSED", "ARCHIVED":
		return true
	default:
		return false
	}
}

func decodeManagerLiveRuntimeStatus(raw, processState string) managerLiveRuntimeStatus {
	view := managerLiveRuntimeStatus{ProcessState: processState}
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return view
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		view.Error = err.Error()
		return view
	}
	view.Status = mapStringValue(payload, "status")
	view.Summary = mapStringValue(payload, "summary")
	view.Paused = mapBoolValue(payload, "paused")
	view.Attachable = mapBoolValue(payload, "attachable")
	view.ActiveTaskID = mapStringValue(payload, "task_id")
	view.ActiveSessionID = mapStringValue(payload, "session_id")
	if control := mapNestedValue(payload, "control"); control != nil {
		view.ControlMode = mapStringValue(control, "mode")
		view.LastAction = mapStringValue(control, "last_action")
		view.LastReason = mapStringValue(control, "last_action_reason")
		if view.ActiveTaskID == "" {
			view.ActiveTaskID = mapStringValue(control, "target_task_id")
		}
	}
	if trigger := mapNestedValue(payload, "work_trigger"); trigger != nil {
		view.Trigger = mapStringValue(trigger, "trigger")
	}
	if packet := mapNestedValue(payload, "work_packet"); packet != nil {
		view.WorkType = mapStringValue(packet, "work_type")
		view.WorkGateReason = mapStringValue(packet, "why_now")
		if gate := mapNestedValue(packet, "gate"); gate != nil {
			view.WorkGateState = mapStringValue(gate, "gate_state")
			view.WorkGateType = mapStringValue(gate, "gate_type")
			view.WorkGateSummary = mapStringValue(gate, "summary")
		}
	}
	if focus := mapNestedValue(payload, "focus"); focus != nil {
		view.FocusTaskID = mapStringValue(focus, "task_id")
		view.FocusTensionID = mapStringValue(focus, "focus_tension_id")
		view.FocusClusterID = mapStringValue(focus, "proto_cluster_id")
	}
	return view
}

func renderLiveRuntimeStatus(status *managerLiveRuntimeStatus) string {
	if status == nil {
		return "  loading...\n"
	}
	var b strings.Builder
	b.WriteString(fmt.Sprintf("  process: %s\n", firstNonEmpty(status.ProcessState, "unknown")))
	if status.Error != "" {
		b.WriteString(fmt.Sprintf("  error: %s\n", status.Error))
		return b.String()
	}
	if status.ProcessState != "running" {
		b.WriteString("  daemon is not running\n")
		return b.String()
	}
	b.WriteString(fmt.Sprintf("  status: %s\n", firstNonEmpty(status.Status, "unknown")))
	b.WriteString(fmt.Sprintf("  summary: %s\n", firstNonEmpty(status.Summary, "-")))
	b.WriteString(fmt.Sprintf("  paused: %t\n", status.Paused))
	b.WriteString(fmt.Sprintf("  attachable: %t\n", status.Attachable))
	b.WriteString(fmt.Sprintf("  task: %s\n", firstNonEmpty(status.ActiveTaskID, "-")))
	b.WriteString(fmt.Sprintf("  session: %s\n", firstNonEmpty(status.ActiveSessionID, "-")))
	b.WriteString(fmt.Sprintf("  control: %s / %s\n", firstNonEmpty(status.ControlMode, "-"), firstNonEmpty(status.LastAction, "-")))
	if status.LastReason != "" {
		b.WriteString(fmt.Sprintf("  reason: %s\n", status.LastReason))
	}
	if status.Trigger != "" {
		b.WriteString(fmt.Sprintf("  trigger: %s\n", status.Trigger))
	}
	if status.WorkType != "" {
		b.WriteString(fmt.Sprintf("  work: %s\n", status.WorkType))
	}
	if status.WorkGateState != "" || status.WorkGateType != "" {
		b.WriteString(fmt.Sprintf("  work gate: %s / %s\n", firstNonEmpty(status.WorkGateState, "-"), firstNonEmpty(status.WorkGateType, "-")))
	}
	if status.WorkGateReason != "" {
		b.WriteString(fmt.Sprintf("  work gate reason: %s\n", status.WorkGateReason))
	}
	if status.WorkGateSummary != "" {
		b.WriteString(fmt.Sprintf("  work gate summary: %s\n", status.WorkGateSummary))
	}
	if status.FocusTaskID != "" || status.FocusTensionID != "" || status.FocusClusterID != "" {
		b.WriteString(fmt.Sprintf("  focus: task=%s tension=%s cluster=%s\n",
			firstNonEmpty(status.FocusTaskID, "-"),
			firstNonEmpty(status.FocusTensionID, "-"),
			firstNonEmpty(status.FocusClusterID, "-"),
		))
	}
	return b.String()
}

func renderWorkspaceCatalog(catalog *managerWorkspaceCatalog, tasks []WorkspaceTaskRecord, tensions []TensionFrontierItem, pickerMode managerPickerMode, pickerIndex int, maxWidth int, taskFilterLabel, tensionFilterLabel, tensionActionLabel string) string {
	if catalog == nil {
		return "  loading...\n"
	}
	var b strings.Builder
	if catalog.Error != "" {
		b.WriteString("  error: " + truncateVisor(catalog.Error, max(16, maxWidth)) + "\n")
	}
	b.WriteString(fmt.Sprintf("  tasks (filter: %s):\n", firstNonEmpty(taskFilterLabel, "all")))
	if len(tasks) == 0 {
		b.WriteString("    (none)\n")
	} else {
		for idx, task := range tasks {
			prefix := "  "
			if pickerMode == managerPickerTask && idx == pickerIndex {
				prefix = "> "
			}
			line := fmt.Sprintf("%s%s [%s] %s", prefix, task.TaskID, firstNonEmpty(task.Status, "-"), firstNonEmpty(task.Title, "-"))
			b.WriteString("    " + truncateVisor(line, max(18, maxWidth)) + "\n")
		}
	}
	b.WriteString(fmt.Sprintf("  tensions (type: %s, action: %s):\n", firstNonEmpty(tensionFilterLabel, "all"), firstNonEmpty(tensionActionLabel, "focus")))
	if len(tensions) == 0 {
		b.WriteString("    (none)\n")
	} else {
		for idx, tension := range tensions {
			prefix := "  "
			if pickerMode == managerPickerTension && idx == pickerIndex {
				prefix = "> "
			}
			line := fmt.Sprintf("%s%s [%s/%s] %s", prefix, tension.TensionID, firstNonEmpty(tension.TensionType, "-"), firstNonEmpty(tension.ReviewStatus, "-"), firstNonEmpty(tension.Title, "-"))
			b.WriteString("    " + truncateVisor(line, max(18, maxWidth)) + "\n")
		}
	}
	return b.String()
}

func renderManagerDefaults(defaults BotManagerDefaults, active, editing bool, selectedIndex int, input string, maxWidth int) string {
	var b strings.Builder
	for idx, spec := range managerDefaultFieldSpecs {
		prefix := "  "
		if active && idx == selectedIndex {
			prefix = "> "
		}
		value := managerDefaultFieldValue(defaults, spec.Key)
		displayValue := value
		if spec.Secret && strings.TrimSpace(displayValue) != "" && !(active && editing && idx == selectedIndex) {
			displayValue = strings.Repeat("*", min(8, max(3, len(strings.TrimSpace(displayValue)))))
		}
		if strings.TrimSpace(displayValue) == "" {
			displayValue = "(empty)"
		}
		if active && editing && idx == selectedIndex {
			displayValue = input
		}
		line := fmt.Sprintf("%s%s: %s", prefix, spec.Label, displayValue)
		b.WriteString("  " + truncateVisor(line, max(20, maxWidth)) + "\n")
	}
	return b.String()
}

func renderManagerCreatePanel(active, editing bool, selectedIndex int, input, parentDir, folderName string, maxWidth int) string {
	var b strings.Builder
	for idx, spec := range managerCreateFieldSpecs {
		prefix := "  "
		if active && idx == selectedIndex {
			prefix = "> "
		}
		value := managerCreateFieldValue(spec.Key, parentDir, folderName)
		displayValue := value
		if spec.Action {
			displayValue = "[enter]"
		}
		if strings.TrimSpace(displayValue) == "" && !spec.Action {
			displayValue = "(empty)"
		}
		if active && editing && idx == selectedIndex && !spec.Action {
			displayValue = input
		}
		line := fmt.Sprintf("%s%s: %s", prefix, spec.Label, displayValue)
		b.WriteString("  " + truncateVisor(line, max(20, maxWidth)) + "\n")
	}
	return b.String()
}

func renderManagerAgentEditor(record ManagedAgentRecord, active, editing bool, selectedIndex int, input string, maxWidth int) string {
	record = normalizeManagedAgentRecord(record)
	var b strings.Builder
	for idx, spec := range managerAgentFieldSpecs {
		prefix := "  "
		if active && idx == selectedIndex {
			prefix = "> "
		}
		value := managerAgentFieldValue(record, spec.Key)
		displayValue := value
		if spec.Action {
			displayValue = "[enter]"
		}
		if strings.TrimSpace(displayValue) == "" && !spec.Action {
			displayValue = "(empty)"
		}
		if active && editing && idx == selectedIndex && !spec.Action {
			displayValue = input
		}
		line := fmt.Sprintf("%s%s: %s", prefix, spec.Label, displayValue)
		if spec.Danger {
			line += " [danger]"
		}
		b.WriteString("  " + truncateVisor(line, max(20, maxWidth)) + "\n")
	}
	return b.String()
}

func managerAgentFieldValue(record ManagedAgentRecord, key string) string {
	switch key {
	case "agent_id":
		return strings.TrimSpace(record.AgentID)
	case "display_name":
		return strings.TrimSpace(record.DisplayName)
	case "workdir":
		return strings.TrimSpace(record.Workdir)
	case "__remove__":
		return ""
	default:
		return ""
	}
}

func managerCreateFieldValue(key, parentDir, folderName string) string {
	switch key {
	case "parent_dir":
		return strings.TrimSpace(parentDir)
	case "folder_name":
		return strings.TrimSpace(folderName)
	case "workdir_preview":
		workdir, err := resolveManagerCreateWorkdir(parentDir, folderName)
		if err != nil {
			return err.Error()
		}
		return workdir
	case "__launch__":
		return ""
	default:
		return ""
	}
}

func managerDefaultFieldValue(defaults BotManagerDefaults, key string) string {
	switch key {
	case "host_url":
		return strings.TrimSpace(defaults.HostURL)
	case "workspace_id":
		return strings.TrimSpace(defaults.WorkspaceID)
	case "workspace_password":
		return strings.TrimSpace(defaults.WorkspacePassword)
	case "owner_user_id":
		return strings.TrimSpace(defaults.OwnerUserID)
	case "llm_backend":
		return strings.TrimSpace(defaults.LLMBackend)
	case "model":
		return strings.TrimSpace(defaults.Model)
	case "role":
		return strings.TrimSpace(defaults.Role)
	case "capabilities":
		return strings.Join(defaults.Capabilities, ",")
	default:
		return ""
	}
}

func cycleManagerOption(current string, options []string) string {
	if len(options) == 0 {
		return strings.TrimSpace(current)
	}
	current = strings.TrimSpace(current)
	for idx, option := range options {
		if strings.EqualFold(strings.TrimSpace(option), current) {
			return options[(idx+1)%len(options)]
		}
	}
	return options[0]
}

func managerFilterLabel(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "all"
	}
	return value
}

func resolveManagerCreateWorkdir(parentDir, folderName string) (string, error) {
	parentDir = strings.TrimSpace(parentDir)
	if parentDir == "" {
		return "", fmt.Errorf("parent_dir is required")
	}
	folderName, err := validateManagedAgentFolderName(folderName)
	if err != nil {
		return "", err
	}
	registry := LoadBotRegistry()
	root, err := managedAgentWorkdirRoot(registry)
	if err != nil {
		return "", err
	}
	parentAbs, err := validateManagedAgentPathWithinRoot(root, parentDir, true)
	if err != nil {
		return "", fmt.Errorf("resolve parent_dir: %w", err)
	}
	return validateManagedAgentWorkdirWithinRoot(root, filepath.Join(parentAbs, folderName))
}

func defaultManagerCreateParentDir(registry BotRegistry, selected int) string {
	if strings.TrimSpace(registry.Defaults.DefaultParentDir) != "" {
		if abs, err := filepath.Abs(strings.TrimSpace(registry.Defaults.DefaultParentDir)); err == nil {
			return abs
		}
		return strings.TrimSpace(registry.Defaults.DefaultParentDir)
	}
	if len(registry.Agents) > 0 {
		if selected >= 0 && selected < len(registry.Agents) {
			workdir := strings.TrimSpace(registry.Agents[selected].Workdir)
			if workdir != "" {
				return filepath.Dir(workdir)
			}
		}
		workdir := strings.TrimSpace(registry.Agents[0].Workdir)
		if workdir != "" {
			return filepath.Dir(workdir)
		}
	}
	home, _ := os.UserHomeDir()
	if strings.TrimSpace(home) == "" {
		return "agents"
	}
	desktopDir := filepath.Join(home, "Desktop")
	if info, err := os.Stat(desktopDir); err == nil && info.IsDir() {
		return filepath.Join(desktopDir, "agents")
	}
	return filepath.Join(home, "agents")
}

func suggestNewAgentFolderName(registry BotRegistry) string {
	used := map[string]struct{}{}
	for _, agent := range registry.Agents {
		base := strings.TrimSpace(filepath.Base(agent.Workdir))
		if base != "" {
			used[strings.ToLower(base)] = struct{}{}
		}
	}
	for idx := 1; idx < 1000; idx++ {
		candidate := fmt.Sprintf("agent-%02d", idx)
		if _, ok := used[strings.ToLower(candidate)]; !ok {
			return candidate
		}
	}
	return "agent-new"
}

func mapNestedValue(payload map[string]any, key string) map[string]any {
	value, _ := payload[key].(map[string]any)
	return value
}

func mapStringValue(payload map[string]any, key string) string {
	value, _ := payload[key].(string)
	return strings.TrimSpace(value)
}

func mapBoolValue(payload map[string]any, key string) bool {
	value, _ := payload[key].(bool)
	return value
}

func joinColumns(left, right string, leftWidth, rightWidth int) string {
	leftLines := strings.Split(strings.TrimRight(left, "\n"), "\n")
	rightLines := strings.Split(strings.TrimRight(right, "\n"), "\n")
	rows := len(leftLines)
	if len(rightLines) > rows {
		rows = len(rightLines)
	}
	var b strings.Builder
	for i := 0; i < rows; i++ {
		leftLine := ""
		rightLine := ""
		if i < len(leftLines) {
			leftLine = truncate(leftLines[i], leftWidth)
		}
		if i < len(rightLines) {
			rightLine = truncate(rightLines[i], rightWidth)
		}
		b.WriteString(padRight(leftLine, leftWidth))
		b.WriteString(" | ")
		b.WriteString(rightLine)
		b.WriteString("\n")
	}
	return strings.TrimRight(b.String(), "\n")
}

func padRight(value string, width int) string {
	if width <= 0 {
		return value
	}
	if len(value) >= width {
		return value
	}
	return value + strings.Repeat(" ", width-len(value))
}

func truncateVisor(value string, max int) string {
	if max <= 0 || len(value) <= max {
		return value
	}
	if max <= 3 {
		return value[:max]
	}
	return value[:max-3] + "..."
}

func runeFromKeyMsg(msg tea.KeyMsg) rune {
	if len(msg.Runes) > 0 {
		return msg.Runes[0]
	}
	if s := msg.String(); len(s) == 1 {
		return rune(s[0])
	}
	return 0
}

func utf8LastRuneInString(s string) (rune, int) {
	for i := len(s) - 1; i >= 0; i-- {
		if (s[i] & 0xC0) != 0x80 {
			r, size := utf8.DecodeRuneInString(s[i:])
			return r, size
		}
	}
	return 0, 0
}

func isInteractiveTerminal(in, out *os.File) bool {
	return isTerminalFile(in) && isTerminalFile(out)
}

func isTerminalFile(f *os.File) bool {
	if f == nil {
		return false
	}
	info, err := f.Stat()
	if err != nil {
		return false
	}
	return info.Mode()&os.ModeCharDevice != 0
}
