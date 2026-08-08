package sqlite

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

var directStoreDBCallExceptions = map[string]string{
	"agent_work_claimability.go:agentWorkAdmissionRevisionSourceBranchBinding:BeginTx": "read-only revision source-branch lookup; reuses claim-admission source-branch predicate before dry-run",
	"agent_work_claimability.go:agentWorkClaimAdmissionBranchByID:BeginTx":             "read-only owner-bound branch binding lookup; canonicalizes claim input before admission dry-run",
	"agent_work_claimability.go:agentWorkClaimAdmissionDryRun:BeginTx":                 "read-only claimability dry-run transaction; delegates read model to claim-admission predicates without writes",
	"agent_work_claimability.go:agentWorkForeignOwnedTaskSelectionBlock:BeginTx":       "read-only owner_user_id hard-claim-route parity check; delegates to rejectForeignAgentOwnedTaskClaimTx (SELECT-only) so the read frontier omits what write admission rejects",
	"projects_coordination.go:GetProjectCoordination:BeginTx":                          "read-only project coordination snapshot transaction; helper path only queries coordination state",
	"projects_patch_queue.go:FirstProjectRepoMutationActivationCandidate:BeginTx":      "read-only repo mutation activation candidate snapshot; helper path only queries queue, branch, repository, and checkout state",
	"projects_patch_queue.go:projectPatchQueueReviewTaskReceipt:BeginTx":               "read-only patch queue review task receipt snapshot; helper path only queries task and runtime event state",
	"anti_gaming_vendoring_gate.go:computeInterpreterVendoringVerdict:BeginTx":         "read-only NO-VENDORING verdict snapshot; helper path only queries the no-vendoring config doc, queue item, and repository before an out-of-tx ground-truth go.mod read",
	"anti_gaming_diff_claim_gate.go:computeDiffClaimDiffStats:BeginTx":                 "read-only diff-implements-claim snapshot; helper path only queries the opt-in config doc, queue item, and repository before an out-of-tx ground-truth base..head numstat read",
	"governance.go:ValidateGovernanceStallPredicates:BeginTx":                          "read-only governance predicate snapshot transaction; helper path only queries project/task/agent state",
	"replica_freshness.go:GetWorkspaceReplicaFreshness:BeginTx":                        "read-only freshness snapshot transaction; helper path only queries state tables",
	"replica_freshness.go:ListWorkspaceReplicaFreshness:BeginTx":                       "read-only freshness list transaction; helper path only queries state tables",
}

func TestProductionWritesDoNotUseStoreReadDB(t *testing.T) {
	t.Parallel()

	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate test file")
	}
	dir := filepath.Dir(currentFile)
	repoRoot := filepath.Clean(filepath.Join(dir, "..", "..", ".."))

	fset := token.NewFileSet()
	var violations []string
	err := filepath.WalkDir(repoRoot, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", ".codex-gotmp", "agent", "knowledge-vault", "vendor":
				return filepath.SkipDir
			default:
				return nil
			}
		}
		if !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", entry.Name(), err)
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			receiverName := ""
			if filepath.Dir(path) == dir {
				receiverName = storeReceiverName(fn)
			}
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				if label, ok := storeDBConstructorInjection(call); ok {
					pos := fset.Position(call.Pos())
					violations = append(violations, fmt.Sprintf("%s:%d %s injects %s; pass Store.WriteDB() or an authority-backed writer", entry.Name(), pos.Line, fn.Name.Name, label))
					return true
				}
				method, ok := "", false
				if receiverName != "" {
					method, ok = directStoreDBMethod(call.Fun, receiverName)
				}
				if !ok {
					method, ok = nestedStoreDBMethod(call.Fun)
				}
				if !ok {
					method, ok = storeDBAccessorMethod(call.Fun)
				}
				if !ok {
					return true
				}
				exceptionKey := fmt.Sprintf("%s:%s:%s", entry.Name(), fn.Name.Name, method)
				if reason := directStoreDBCallExceptions[exceptionKey]; reason != "" {
					return true
				}
				if strings.HasPrefix(method, "Exec") {
					if sqlText, ok := directDBExecSQLLiteral(call, method); ok && directDBExecIsReadOnly(sqlText) {
						return true
					}
				}
				pos := fset.Position(call.Pos())
				violations = append(violations, fmt.Sprintf("%s:%d %s uses s.db.%s; route production writes through s.writeDB or BeginTxImmediate", entry.Name(), pos.Line, fn.Name.Name, method))
				return true
			})
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk repository: %v", err)
	}
	if len(violations) > 0 {
		t.Fatalf("direct Store.db production write path(s):\n%s", strings.Join(violations, "\n"))
	}
}

func storeReceiverName(fn *ast.FuncDecl) string {
	if fn.Recv == nil || len(fn.Recv.List) == 0 {
		return ""
	}
	field := fn.Recv.List[0]
	if len(field.Names) == 0 {
		return ""
	}
	typ := field.Type
	if ptr, ok := typ.(*ast.StarExpr); ok {
		typ = ptr.X
	}
	ident, ok := typ.(*ast.Ident)
	if !ok || ident.Name != "Store" {
		return ""
	}
	return field.Names[0].Name
}

func directStoreDBMethod(fun ast.Expr, receiverName string) (string, bool) {
	sel, ok := fun.(*ast.SelectorExpr)
	if !ok {
		return "", false
	}
	if !isDBWriteMethod(sel.Sel.Name) {
		return "", false
	}
	dbSelector, ok := sel.X.(*ast.SelectorExpr)
	if !ok || dbSelector.Sel.Name != "db" {
		return "", false
	}
	receiver, ok := dbSelector.X.(*ast.Ident)
	if !ok || receiver.Name != receiverName {
		return "", false
	}
	return sel.Sel.Name, true
}

func nestedStoreDBMethod(fun ast.Expr) (string, bool) {
	sel, ok := fun.(*ast.SelectorExpr)
	if !ok || !isDBWriteMethod(sel.Sel.Name) {
		return "", false
	}
	dbSelector, ok := sel.X.(*ast.SelectorExpr)
	if !ok || dbSelector.Sel.Name != "db" {
		return "", false
	}
	storeSelector, ok := dbSelector.X.(*ast.SelectorExpr)
	if !ok || storeSelector.Sel.Name != "store" {
		return "", false
	}
	return sel.Sel.Name, true
}

func storeDBAccessorMethod(fun ast.Expr) (string, bool) {
	sel, ok := fun.(*ast.SelectorExpr)
	if !ok || !isDBWriteMethod(sel.Sel.Name) {
		return "", false
	}
	call, ok := sel.X.(*ast.CallExpr)
	if !ok {
		return "", false
	}
	callee, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || callee.Sel.Name != "DB" {
		return "", false
	}
	if !isLikelySQLiteStoreAccessor(callee.X) {
		return "", false
	}
	return sel.Sel.Name, true
}

func storeDBConstructorInjection(call *ast.CallExpr) (string, bool) {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != "NewStore" {
		return "", false
	}
	pkg, ok := sel.X.(*ast.Ident)
	if !ok || pkg.Name != "mcp" {
		return "", false
	}
	for _, arg := range call.Args {
		if isStoreDBAccessorCall(arg) {
			return "mcp.NewStore(Store.DB())", true
		}
	}
	return "", false
}

func isStoreDBAccessorCall(expr ast.Expr) bool {
	call, ok := expr.(*ast.CallExpr)
	if !ok {
		return false
	}
	callee, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || callee.Sel.Name != "DB" {
		return false
	}
	return isLikelySQLiteStoreAccessor(callee.X)
}

func isLikelySQLiteStoreAccessor(expr ast.Expr) bool {
	switch v := expr.(type) {
	case *ast.Ident:
		return strings.EqualFold(v.Name, "store")
	case *ast.SelectorExpr:
		return strings.EqualFold(v.Sel.Name, "store")
	default:
		return false
	}
}

func isDBWriteMethod(method string) bool {
	switch method {
	case "Exec", "ExecContext", "Begin", "BeginTx":
		return true
	default:
		return false
	}
}

func directDBExecSQLLiteral(call *ast.CallExpr, method string) (string, bool) {
	argIndex := 0
	if method == "ExecContext" {
		argIndex = 1
	}
	if len(call.Args) <= argIndex {
		return "", false
	}
	return sqlStringLiteral(call.Args[argIndex])
}

func sqlStringLiteral(expr ast.Expr) (string, bool) {
	switch v := expr.(type) {
	case *ast.BasicLit:
		if v.Kind != token.STRING {
			return "", false
		}
		text, err := strconv.Unquote(v.Value)
		if err != nil {
			return "", false
		}
		return text, true
	case *ast.ParenExpr:
		return sqlStringLiteral(v.X)
	case *ast.BinaryExpr:
		if v.Op != token.ADD {
			return "", false
		}
		left, ok := sqlStringLiteral(v.X)
		if !ok {
			return "", false
		}
		right, ok := sqlStringLiteral(v.Y)
		if !ok {
			return "", false
		}
		return left + right, true
	default:
		return "", false
	}
}

func directDBExecIsReadOnly(sqlText string) bool {
	keyword := firstSQLKeyword(sqlText)
	switch keyword {
	case "EXPLAIN", "SELECT":
		return true
	default:
		return false
	}
}

func firstSQLKeyword(sqlText string) string {
	text := strings.TrimSpace(sqlText)
	for {
		switch {
		case strings.HasPrefix(text, "--"):
			if idx := strings.IndexByte(text, '\n'); idx >= 0 {
				text = strings.TrimSpace(text[idx+1:])
				continue
			}
			return ""
		case strings.HasPrefix(text, "/*"):
			if idx := strings.Index(text, "*/"); idx >= 0 {
				text = strings.TrimSpace(text[idx+2:])
				continue
			}
			return ""
		default:
			fields := strings.Fields(text)
			if len(fields) == 0 {
				return ""
			}
			return strings.ToUpper(strings.Trim(fields[0], "(`"))
		}
	}
}
