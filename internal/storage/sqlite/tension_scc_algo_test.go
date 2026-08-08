package sqlite

import (
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestTarjanSCC(t *testing.T) {
	tests := []struct {
		name     string
		graph    map[string][]string
		expected [][]string // Components of size > 1 are the ones we care about, but tarjan returns all
	}{
		{
			name: "linear no cycles",
			graph: map[string][]string{
				"A": {"B"},
				"B": {"C"},
				"C": {},
			},
			expected: [][]string{
				{"C"},
				{"B"},
				{"A"},
			},
		},
		{
			name: "simple cycle",
			graph: map[string][]string{
				"A": {"B"},
				"B": {"C"},
				"C": {"A"},
			},
			expected: [][]string{
				{"A", "B", "C"},
			},
		},
		{
			name: "two separate cycles",
			graph: map[string][]string{
				"A": {"B"},
				"B": {"A"},
				"C": {"D"},
				"D": {"C"},
			},
			expected: [][]string{
				{"C", "D"},
				{"A", "B"},
			},
		},
		{
			name: "cycle with tails",
			graph: map[string][]string{
				"1": {"2"},
				"2": {"3"},
				"3": {"4", "5"},
				"4": {"2"},
				"5": {"6"},
			},
			expected: [][]string{
				{"6"},
				{"5"},
				{"2", "3", "4"},
				{"1"},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := tarjanSCC(tc.graph)
			// Sort inner slices for comparison
			for i := range got {
				sort.Strings(got[i])
			}
			for i := range tc.expected {
				sort.Strings(tc.expected[i])
			}
			// Sort outer slices by joining elements
			sort.Slice(got, func(i, j int) bool {
				return got[i][0] < got[j][0]
			})
			sort.Slice(tc.expected, func(i, j int) bool {
				return tc.expected[i][0] < tc.expected[j][0]
			})

			if diff := cmp.Diff(tc.expected, got); diff != "" {
				t.Fatalf("tarjanSCC mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
