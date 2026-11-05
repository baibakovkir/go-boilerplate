// Команда style форматирует исходники и проверяет общие правила оформления.
package main

import (
	"bytes"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

const maxColumns = 120

func main() {
	check := flag.Bool("check", false, "проверить оформление без изменения файлов")
	flag.Parse()
	if err := run(*check); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(check bool) error {
	failed := false
	for _, root := range []string{"cmd", "internal"} {
		err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() || !strings.HasSuffix(path, ".go") {
				return nil
			}
			source, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			// sqlc отвечает за оформление собственных файлов; результат генерации не меняем.
			if generated(source) {
				return nil
			}
			formatted := source
			commands := [][]string{
				{"goimports", "-local", "go-boilerplate", "-srcdir", path},
				{"golines", "--base-formatter=gofmt", "--no-reformat-tags", "--max-len=120", "--tab-len=4"},
				{"gofumpt"},
			}
			for _, args := range commands {
				command := exec.Command("go", append([]string{"tool"}, args...)...)
				command.Stdin = bytes.NewReader(formatted)
				var stderr bytes.Buffer
				command.Stderr = &stderr
				formatted, err = command.Output()
				if err != nil {
					return fmt.Errorf("%s: %s: %w\n%s", path, args[0], err, stderr.String())
				}
			}
			if !bytes.Equal(source, formatted) {
				if check {
					fmt.Fprintf(os.Stderr, "%s: требуется make fmt\n", path)
					failed = true
				} else if err = os.WriteFile(path, formatted, 0o644); err != nil {
					return err
				}
			}
			for index, line := range strings.Split(string(formatted), "\n") {
				if columns(line) > maxColumns {
					fmt.Fprintf(
						os.Stderr,
						"%s:%d: длина строки %d, максимум %d\n",
						path,
						index+1,
						columns(line),
						maxColumns,
					)
					failed = true
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	if failed {
		return fmt.Errorf("проверка оформления не пройдена")
	}
	return nil
}

func generated(source []byte) bool {
	for _, line := range strings.Split(string(source), "\n") {
		if strings.HasPrefix(line, "// Code generated ") && strings.HasSuffix(line, " DO NOT EDIT.") {
			return true
		}
	}
	return false
}

func columns(line string) int {
	width := 0
	for _, symbol := range line {
		// Считаем символы Unicode, а не байты: русский текст не должен считаться вдвое длиннее.
		if symbol == '\t' {
			width += 4 - width%4
		} else {
			width++
		}
	}
	return width
}
