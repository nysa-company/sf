package processsupervisor

import (
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"testing"
)

func TestAuthoringPreparationCategoriesDoNotExposeCauses(t *testing.T) {
	for _, stage := range []string{"unsupported", "resolve", "staging", "authlookup", "authrenewal", "version", "help", "authstatus", "binding", "supervisor_state"} {
		err := preparationFailure(stage, errCLIObservation)
		if AuthoringPreparationCategory(fmt.Errorf("wrapped: %w", err)) != stage || !errors.Is(err, errCLIObservation) {
			t.Fatal("lost closed stage or sentinel")
		}
		unsafe := preparationFailure(stage, errors.New("secret-token\x1b[31m"))
		if strings.Contains(unsafe.Error(), "secret") || strings.Contains(errors.Unwrap(unsafe).Error(), "secret") {
			t.Fatal("arbitrary cause retained")
		}
	}
	for _, err := range []error{nil, errors.New("secret-token"), preparationFailure("secret-token", errCLIObservation)} {
		if AuthoringPreparationCategory(err) != "unknown" {
			t.Fatal("unrecognized category exposed")
		}
	}
	if !errors.Is(preparationFailure("authrenewal", errClaudeAuthRenewal), errClaudeAuthRenewal) || !errors.Is(preparationFailure("supervisor_state", ErrUnclear), ErrUnclear) {
		t.Fatal("known sentinel identity lost")
	}
}

func TestInstalledAuthoringPrepareOnlyIsExclusiveFirstBranch(t *testing.T) {
	file, err := parser.ParseFile(token.NewFileSet(), "authoring_installed_test.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var entry, prepare *ast.FuncDecl
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok {
			continue
		}
		switch fn.Name.Name {
		case "TestInstalledClaudeAuthoringPurposes":
			entry = fn
		case "installedClaudeAuthoringPreparationOnly":
			prepare = fn
		}
	}
	if entry == nil || prepare == nil {
		t.Fatal("missing preparation branch")
	}
	branch, ok := entry.Body.List[0].(*ast.IfStmt)
	if !ok || len(branch.Body.List) != 2 {
		t.Fatal("preparation must be exclusive first branch")
	}
	condition, ok := branch.Cond.(*ast.BinaryExpr)
	if !ok || condition.Op != token.EQL {
		t.Fatal("preparation gate must be exact")
	}
	call, ok := condition.X.(*ast.CallExpr)
	if !ok || len(call.Args) != 1 {
		t.Fatal("missing environment gate")
	}
	literal, ok := call.Args[0].(*ast.BasicLit)
	if !ok || literal.Value != `"SF_TEST_CLAUDE_AUTHORING_PREPARE_ONLY"` {
		t.Fatal("wrong preparation gate")
	}
	value, ok := condition.Y.(*ast.BasicLit)
	if !ok || value.Value != `"1"` {
		t.Fatal("preparation gate must require 1")
	}
	if _, ok := branch.Body.List[1].(*ast.ReturnStmt); !ok {
		t.Fatal("preparation can fall through into inference")
	}
	statement, ok := branch.Body.List[0].(*ast.ExprStmt)
	if !ok {
		t.Fatal("missing direct preparation helper")
	}
	invoke, ok := statement.X.(*ast.CallExpr)
	if !ok {
		t.Fatal("missing preparation invocation")
	}
	name, ok := invoke.Fun.(*ast.Ident)
	if !ok || name.Name != "installedClaudeAuthoringPreparationOnly" {
		t.Fatal("wrong preparation helper")
	}
	seenPrepare := false
	ast.Inspect(prepare.Body, func(node ast.Node) bool {
		invocation, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		if identifier, ok := invocation.Fun.(*ast.Ident); ok && identifier.Name == "cancel" {
			return true
		}
		if _, ok := invocation.Fun.(*ast.FuncLit); ok {
			return true
		}
		selector, ok := invocation.Fun.(*ast.SelectorExpr)
		if !ok {
			t.Fatal("unexpected indirect preparation call")
			return false
		}
		switch selector.Sel.Name {
		case "Helper", "Fatal", "Error", "Log", "WithTimeout", "Background", "Second", "New", "Close", "AuthoringPreparationCategory":
		case "PrepareAuthoring":
			seenPrepare = true
		default:
			t.Fatalf("unexpected preparation call: %s", selector.Sel.Name)
		}
		return true
	})
	if !seenPrepare {
		t.Fatal("preparation path missing actual observer")
	}
}
