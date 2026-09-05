// Command check-go-discards rejects unexplained blank-identifier assignments in production Go code.
package main

import (
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

type finding struct {
	file string
	line int
	text string
}

type counts struct {
	assignments int
	mechanics   int
	justified   int
}

func main() {
	root := flag.String("root", ".", "repository root")
	flag.Parse()

	result, unexplained, err := audit(*root)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	for _, item := range unexplained {
		fmt.Printf("%s:%d: %s\n", item.file, item.line, item.text)
	}
	fmt.Printf("production blank assignments: %d (language mechanics: %d, explained: %d, unexplained: %d)\n",
		result.assignments, result.mechanics, result.justified, len(unexplained))
	if len(unexplained) != 0 {
		os.Exit(1)
	}
}

func audit(root string) (counts, []finding, error) {
	var result counts
	var unexplained []finding
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if entry.Name() == ".git" || entry.Name() == "target" || entry.Name() == "vendor" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") || strings.HasSuffix(path, ".pb.go") {
			return nil
		}
		fileCounts, fileFindings, err := auditFile(root, path)
		result.assignments += fileCounts.assignments
		result.mechanics += fileCounts.mechanics
		result.justified += fileCounts.justified
		unexplained = append(unexplained, fileFindings...)
		return err
	})
	sort.Slice(unexplained, func(i, j int) bool {
		if unexplained[i].file == unexplained[j].file {
			return unexplained[i].line < unexplained[j].line
		}
		return unexplained[i].file < unexplained[j].file
	})
	return result, unexplained, err
}

func auditFile(root, path string) (counts, []finding, error) {
	source, err := os.ReadFile(path)
	if err != nil {
		return counts{}, nil, err
	}
	set := token.NewFileSet()
	parsed, err := parser.ParseFile(set, path, source, parser.ParseComments)
	if err != nil {
		return counts{}, nil, fmt.Errorf("parse %s: %w", path, err)
	}

	commentLines := make(map[int]string)
	for _, group := range parsed.Comments {
		for _, comment := range group.List {
			line := set.Position(comment.Pos()).Line
			commentLines[line] = strings.ToLower(comment.Text)
		}
	}

	var result counts
	var findings []finding
	lines := strings.Split(string(source), "\n")
	ast.Inspect(parsed, func(node ast.Node) bool {
		assignment, ok := node.(*ast.AssignStmt)
		if !ok || assignment.Tok != token.ASSIGN || !hasTrailingBlankIdentifier(assignment.Lhs) {
			return true
		}
		result.assignments++
		if isLanguageMechanic(assignment) {
			result.mechanics++
			return true
		}
		line := set.Position(assignment.Pos()).Line
		if hasExplanation(commentLines, line) {
			result.justified++
			return true
		}
		text := strings.TrimSpace(lines[line-1])
		findings = append(findings, finding{file: relative(root, path), line: line, text: text})
		return true
	})
	return result, findings, nil
}

func hasTrailingBlankIdentifier(expressions []ast.Expr) bool {
	if len(expressions) == 0 {
		return false
	}
	identifier, ok := expressions[len(expressions)-1].(*ast.Ident)
	return ok && identifier.Name == "_"
}

func isLanguageMechanic(assignment *ast.AssignStmt) bool {
	if len(assignment.Lhs) == 1 && len(assignment.Rhs) == 1 {
		_, blank := assignment.Lhs[0].(*ast.Ident)
		_, identifier := assignment.Rhs[0].(*ast.Ident)
		return blank && identifier
	}
	if len(assignment.Lhs) <= 1 || len(assignment.Rhs) != 1 {
		return false
	}
	call, ok := assignment.Rhs[0].(*ast.CallExpr)
	if !ok {
		return false
	}
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	packageName, ok := selector.X.(*ast.Ident)
	if !ok {
		return false
	}
	qualified := packageName.Name + "." + selector.Sel.Name
	switch qualified {
	case "strings.Cut", "syscall.Syscall", "syscall.Syscall6", "syscall.Syscall9":
		return true
	default:
		return false
	}
}

func hasExplanation(comments map[int]string, line int) bool {
	for _, candidate := range []int{line - 1, line} {
		comment := comments[candidate]
		if comment == "" {
			continue
		}
		if strings.Contains(comment, "ignore error") || strings.Contains(comment, "ignore errors") {
			continue
		}
		return true
	}
	return false
}

func relative(root, path string) string {
	value, err := filepath.Rel(root, path)
	if err != nil {
		return filepath.ToSlash(path)
	}
	return filepath.ToSlash(value)
}
