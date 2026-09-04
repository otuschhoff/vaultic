// Command codemetrics reports reproducible source-size and complexity metrics.
package main

import (
	"bufio"
	"errors"
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

const (
	longFunctionLines     = 100
	veryLongFunctionLines = 150
	largeFileLines        = 800
	longLineColumns       = 200
)

type functionMetric struct {
	file       string
	name       string
	lines      int
	complexity int
	parameters int
}

type fileMetric struct {
	file  string
	lines int
}

type report struct {
	goProductionFiles int
	goTestFiles       int
	goProductionLines int
	goFunctions       []functionMetric
	goFiles           []fileMetric
	goLongLines       int
	rustFiles         []fileMetric
}

func main() {
	root := flag.String("root", ".", "repository root")
	top := flag.Int("top", 20, "number of hotspots to print")
	flag.Parse()

	metrics, err := collect(*root)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	printReport(metrics, *top)
}

func collect(root string) (report, error) {
	var result report
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
		if strings.HasSuffix(path, ".go") {
			return collectGoFile(root, path, &result)
		}
		if strings.HasSuffix(path, ".rs") {
			lines, err := countLines(path)
			if err != nil {
				return err
			}
			result.rustFiles = append(result.rustFiles, fileMetric{file: relative(root, path), lines: lines})
		}
		return nil
	})
	return result, err
}

func collectGoFile(root, path string, result *report) error {
	lines, err := countLines(path)
	if err != nil {
		return err
	}
	if strings.HasSuffix(path, "_test.go") {
		result.goTestFiles++
		return nil
	}
	if strings.HasSuffix(path, ".pb.go") {
		return nil
	}
	result.goProductionFiles++
	result.goProductionLines += lines
	result.goFiles = append(result.goFiles, fileMetric{file: relative(root, path), lines: lines})

	file, err := os.Open(path)
	if err != nil {
		return err
	}
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	for scanner.Scan() {
		if len(scanner.Bytes()) > longLineColumns {
			result.goLongLines++
		}
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := scanner.Err(); err != nil {
		return err
	}

	set := token.NewFileSet()
	parsed, err := parser.ParseFile(set, path, nil, 0)
	if err != nil {
		return fmt.Errorf("parse %s: %w", path, err)
	}
	for _, declaration := range parsed.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Body == nil {
			continue
		}
		result.goFunctions = append(result.goFunctions, functionStats(set, relative(root, path), function))
	}
	return nil
}

func functionStats(set *token.FileSet, file string, function *ast.FuncDecl) functionMetric {
	name := function.Name.Name
	if function.Recv != nil && len(function.Recv.List) > 0 {
		name = receiverName(function.Recv.List[0].Type) + "." + name
	}
	parameters := 0
	for _, parameter := range function.Type.Params.List {
		if len(parameter.Names) == 0 {
			parameters++
		} else {
			parameters += len(parameter.Names)
		}
	}
	complexity := 1
	ast.Inspect(function.Body, func(node ast.Node) bool {
		switch expression := node.(type) {
		case *ast.IfStmt, *ast.ForStmt, *ast.RangeStmt, *ast.CaseClause, *ast.CommClause:
			complexity++
		case *ast.BinaryExpr:
			if expression.Op == token.LAND || expression.Op == token.LOR {
				complexity++
			}
		}
		return true
	})
	return functionMetric{
		file:       file,
		name:       name,
		lines:      set.Position(function.End()).Line - set.Position(function.Pos()).Line + 1,
		complexity: complexity,
		parameters: parameters,
	}
}

func receiverName(expression ast.Expr) string {
	switch value := expression.(type) {
	case *ast.Ident:
		return value.Name
	case *ast.StarExpr:
		return receiverName(value.X)
	case *ast.IndexExpr:
		return receiverName(value.X)
	case *ast.IndexListExpr:
		return receiverName(value.X)
	default:
		return "?"
	}
}

func printReport(metrics report, top int) {
	longFunctions := 0
	veryLongFunctions := 0
	manyParameters := 0
	for _, function := range metrics.goFunctions {
		if function.lines > longFunctionLines {
			longFunctions++
		}
		if function.lines > veryLongFunctionLines {
			veryLongFunctions++
		}
		if function.parameters >= 8 {
			manyParameters++
		}
	}
	largeFiles := 0
	for _, file := range metrics.goFiles {
		if file.lines > largeFileLines {
			largeFiles++
		}
	}

	fmt.Println("# Vaultic code metrics")
	fmt.Printf("Go production files (excluding generated protobuf): %d\n", metrics.goProductionFiles)
	fmt.Printf("Go test files: %d\n", metrics.goTestFiles)
	fmt.Printf("Go production LOC: %d\n", metrics.goProductionLines)
	fmt.Printf("Go functions: %d\n", len(metrics.goFunctions))
	fmt.Printf("Go functions > %d lines: %d\n", longFunctionLines, longFunctions)
	fmt.Printf("Go functions > %d lines: %d\n", veryLongFunctionLines, veryLongFunctions)
	fmt.Printf("Go functions with >= 8 parameters: %d\n", manyParameters)
	fmt.Printf("Go files > %d lines: %d\n", largeFileLines, largeFiles)
	fmt.Printf("Go lines > %d bytes: %d\n", longLineColumns, metrics.goLongLines)
	fmt.Printf("Rust files: %d\n", len(metrics.rustFiles))
	fmt.Printf("Rust LOC: %d\n", sumLines(metrics.rustFiles))

	sort.Slice(metrics.goFunctions, func(i, j int) bool {
		if metrics.goFunctions[i].complexity == metrics.goFunctions[j].complexity {
			return metrics.goFunctions[i].lines > metrics.goFunctions[j].lines
		}
		return metrics.goFunctions[i].complexity > metrics.goFunctions[j].complexity
	})
	fmt.Printf("\n## Top %d Go function hotspots\n", min(top, len(metrics.goFunctions)))
	fmt.Println("complexity\tlines\tparameters\tfunction\tfile")
	for _, function := range metrics.goFunctions[:min(top, len(metrics.goFunctions))] {
		fmt.Printf("%d\t%d\t%d\t%s\t%s\n", function.complexity, function.lines, function.parameters, function.name, function.file)
	}

	allFiles := append(append([]fileMetric(nil), metrics.goFiles...), metrics.rustFiles...)
	sort.Slice(allFiles, func(i, j int) bool { return allFiles[i].lines > allFiles[j].lines })
	fmt.Printf("\n## Top %d production source files\n", min(top, len(allFiles)))
	fmt.Println("lines\tfile")
	for _, file := range allFiles[:min(top, len(allFiles))] {
		fmt.Printf("%d\t%s\n", file.lines, file.file)
	}
}

func countLines(path string) (int, error) {
	file, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	lines := 0
	for scanner.Scan() {
		lines++
	}
	scanErr := scanner.Err()
	return lines, errors.Join(scanErr, file.Close())
}

func sumLines(files []fileMetric) int {
	total := 0
	for _, file := range files {
		total += file.lines
	}
	return total
}

func relative(root, path string) string {
	value, err := filepath.Rel(root, path)
	if err != nil {
		return filepath.ToSlash(path)
	}
	return filepath.ToSlash(value)
}
