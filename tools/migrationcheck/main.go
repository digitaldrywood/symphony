package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"strconv"
	"strings"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("migrationcheck", flag.ContinueOnError)
	flags.SetOutput(stderr)
	root := flags.String("root", ".", "repository root containing migration directories")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if flags.NArg() != 0 {
		fmt.Fprintln(stderr, "migrationcheck accepts only -root")
		return 2
	}
	if err := checkMigrations(os.DirFS(*root), []string{"internal/store/migrations", "internal/hubserver/migrations"}); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	fmt.Fprintln(stdout, "Migration versions are unique within each schema.")
	return 0
}

func checkMigrations(root fs.FS, directories []string) error {
	var failures []error
	for _, directory := range directories {
		files, err := fs.ReadDir(root, directory)
		if err != nil {
			failures = append(failures, fmt.Errorf("read migrations: %w", err))
			continue
		}
		versions := make(map[int64]string)
		for _, file := range files {
			if file.IsDir() || path.Ext(file.Name()) != ".sql" {
				continue
			}
			name := path.Join(directory, file.Name())
			prefix, _, ok := strings.Cut(file.Name(), "_")
			version, err := strconv.ParseInt(prefix, 10, 64)
			if !ok || err != nil || version <= 0 {
				failures = append(failures, fmt.Errorf("invalid migration version: %s", name))
				continue
			}
			if previous, exists := versions[version]; exists {
				failures = append(failures, fmt.Errorf("duplicate migration version %d: %s and %s", version, previous, name))
			}
			versions[version] = name
		}
		if len(versions) == 0 {
			failures = append(failures, fmt.Errorf("no SQL migrations in %s", directory))
		}
	}
	return errors.Join(failures...)
}
