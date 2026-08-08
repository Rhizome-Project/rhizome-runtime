package sessionmemory

import "testing"

func TestMemoryScoresTreatAntiProcedureLikeProcedure(t *testing.T) {
	t.Parallel()

	procedureImportance, procedureConfidence := memoryScores("PROCEDURE", "manual")
	antiImportance, antiConfidence := memoryScores("ANTI_PROCEDURE", "manual")

	if antiImportance != procedureImportance || antiConfidence != procedureConfidence {
		t.Fatalf("expected anti procedure to inherit procedure score parity, procedure=(%.2f, %.2f) anti=(%.2f, %.2f)", procedureImportance, procedureConfidence, antiImportance, antiConfidence)
	}
}
