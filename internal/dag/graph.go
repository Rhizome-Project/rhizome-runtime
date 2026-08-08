package dag

import (
	"errors"
	"fmt"
	"slices"
	"strings"
)

type NodeSpec struct {
	NodeID    string   `json:"node_id"`
	Type      string   `json:"type"`
	DependsOn []string `json:"depends_on"`
}

type Graph struct {
	Nodes []NodeSpec `json:"nodes"`
}

func DefaultGraph() Graph {
	return Graph{
		Nodes: []NodeSpec{
			{
				NodeID:    "node-1",
				Type:      "generic",
				DependsOn: []string{},
			},
		},
	}
}

func NormalizeGraph(g Graph) Graph {
	out := Graph{Nodes: make([]NodeSpec, 0, len(g.Nodes))}

	for _, node := range g.Nodes {
		id := strings.TrimSpace(node.NodeID)
		kind := strings.TrimSpace(node.Type)
		if kind == "" {
			kind = "generic"
		}

		deps := make([]string, 0, len(node.DependsOn))
		seen := make(map[string]struct{}, len(node.DependsOn))
		for _, dep := range node.DependsOn {
			d := strings.TrimSpace(dep)
			if d == "" {
				continue
			}
			if _, ok := seen[d]; ok {
				continue
			}
			seen[d] = struct{}{}
			deps = append(deps, d)
		}
		slices.Sort(deps)

		out.Nodes = append(out.Nodes, NodeSpec{
			NodeID:    id,
			Type:      kind,
			DependsOn: deps,
		})
	}

	return out
}

func ValidateGraph(g Graph) error {
	if len(g.Nodes) == 0 {
		return errors.New("graph must contain at least one node")
	}

	nodeIndex := make(map[string]int, len(g.Nodes))
	for i, node := range g.Nodes {
		if node.NodeID == "" {
			return fmt.Errorf("node at index %d has empty node_id", i)
		}
		if _, exists := nodeIndex[node.NodeID]; exists {
			return fmt.Errorf("duplicate node_id: %s", node.NodeID)
		}
		nodeIndex[node.NodeID] = i
	}

	for _, node := range g.Nodes {
		for _, dep := range node.DependsOn {
			if dep == node.NodeID {
				return fmt.Errorf("node %s cannot depend on itself", node.NodeID)
			}
			if _, exists := nodeIndex[dep]; !exists {
				return fmt.Errorf("node %s depends on unknown node %s", node.NodeID, dep)
			}
		}
	}

	// Cycle detection via DFS over dependency edges.
	visiting := make(map[string]bool, len(g.Nodes))
	visited := make(map[string]bool, len(g.Nodes))

	var visit func(string) error
	visit = func(nodeID string) error {
		if visited[nodeID] {
			return nil
		}
		if visiting[nodeID] {
			return fmt.Errorf("cycle detected at node %s", nodeID)
		}
		visiting[nodeID] = true

		node := g.Nodes[nodeIndex[nodeID]]
		for _, dep := range node.DependsOn {
			if err := visit(dep); err != nil {
				return err
			}
		}

		visiting[nodeID] = false
		visited[nodeID] = true
		return nil
	}

	for _, node := range g.Nodes {
		if err := visit(node.NodeID); err != nil {
			return err
		}
	}

	return nil
}
