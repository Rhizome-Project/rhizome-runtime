package plugins

import (
	"fmt"

	"github.com/Rhizome-Project/rhizome-runtime/internal/agent/hooks"
	"github.com/Rhizome-Project/rhizome-runtime/internal/agent/tools"
)

// Plugin defines the interface for an agent plugin.
type Plugin interface {
	Name() string
	Description() string
	Tools() []tools.Tool
	Hooks() []hooks.Hook
	PromptFragments() []string
}

// Registry holds registered plugins.
type Registry struct {
	plugins []Plugin
	byName  map[string]Plugin
}

// NewRegistry creates a new plugin registry.
func NewRegistry() *Registry {
	return &Registry{
		plugins: make([]Plugin, 0),
		byName:  make(map[string]Plugin),
	}
}

// Register adds a plugin. Returns error if name is already taken.
func (r *Registry) Register(plugin Plugin) error {
	name := plugin.Name()
	if _, exists := r.byName[name]; exists {
		return fmt.Errorf("plugin already registered: %s", name)
	}
	r.plugins = append(r.plugins, plugin)
	r.byName[name] = plugin
	return nil
}

// Get returns a plugin by name.
func (r *Registry) Get(name string) (Plugin, bool) {
	p, ok := r.byName[name]
	return p, ok
}

// List returns all registered plugins in registration order.
func (r *Registry) List() []Plugin {
	out := make([]Plugin, len(r.plugins))
	copy(out, r.plugins)
	return out
}

// AllTools returns all tools from all plugins in registration order.
func (r *Registry) AllTools() []tools.Tool {
	var all []tools.Tool
	for _, p := range r.plugins {
		all = append(all, p.Tools()...)
	}
	if all == nil {
		all = []tools.Tool{}
	}
	return all
}

// AllHooks returns all hooks from all plugins in registration order.
func (r *Registry) AllHooks() []hooks.Hook {
	var all []hooks.Hook
	for _, p := range r.plugins {
		all = append(all, p.Hooks()...)
	}
	if all == nil {
		all = []hooks.Hook{}
	}
	return all
}

// AllPromptFragments returns all prompt fragments from all plugins.
func (r *Registry) AllPromptFragments() []string {
	var all []string
	for _, p := range r.plugins {
		all = append(all, p.PromptFragments()...)
	}
	if all == nil {
		all = []string{}
	}
	return all
}
