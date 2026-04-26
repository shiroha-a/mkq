// Package lua loads and resolves BullMQ-style Lua scripts.
//
// The vendored scripts under scripts/ are taken verbatim from BullMQ
// (see scripts/UPSTREAM.md). They use a custom directive of the form
//
//	--- @include "path"
//
// which BullMQ resolves at build time via its TypeScript script-loader.
// mkq performs the same resolution at runtime in pure Go so we can vendor
// the scripts without a build step.
package lua

import (
	"bytes"
	"embed"
	"fmt"
	"io/fs"
	"path"
	"regexp"
	"strings"
)

// `all:scripts` walks the directory recursively and includes hidden
// files, so any future re-vendoring that adds a deeper sub-directory
// is picked up without touching this directive.
//
//go:embed all:scripts
var scriptsFS embed.FS

const scriptsRoot = "scripts"

// includeRe matches a BullMQ include directive. The captured group is the
// referenced path, which is resolved relative to scripts/ for entry-point
// scripts and relative to scripts/includes/ for nested includes.
var includeRe = regexp.MustCompile(`(?m)^\s*---\s*@include\s+"([^"]+)"\s*$`)

// maxIncludeDepth caps recursion to surface accidental cycles. BullMQ's
// own dependency graph is shallow (depth 4 at most as of the pinned
// commit), so 32 leaves comfortable headroom.
const maxIncludeDepth = 32

// Preprocess returns the content of the named entry-point script with all
// transitively reachable @include directives expanded inline. The name
// must be a basename without extension (e.g. "addStandardJob-9").
//
// Each include is inlined exactly once even if referenced multiple times,
// matching BullMQ's script-loader behavior.
func Preprocess(name string) (string, error) {
	return preprocess(scriptsFS, name)
}

// preprocess is the testable core. It is parameterised on fs so tests
// can supply a synthetic filesystem.
func preprocess(fsys fs.FS, name string) (string, error) {
	entry := path.Join(scriptsRoot, name+".lua")
	included := make(map[string]bool)
	stack := make(map[string]bool)
	var buf bytes.Buffer
	if err := expand(fsys, entry, &buf, included, stack, 0); err != nil {
		return "", err
	}
	return buf.String(), nil
}

// expand reads filePath from fsys and writes its contents into buf,
// recursively replacing each @include directive with the referenced
// file's expansion.
//
// included tracks files already inlined so we emit each at most once.
// stack tracks the current recursion path so we can detect cycles
// regardless of the include-once semantics.
func expand(fsys fs.FS, filePath string, buf *bytes.Buffer, included, stack map[string]bool, depth int) error {
	if depth > maxIncludeDepth {
		return fmt.Errorf("lua include depth exceeded %d at %s", maxIncludeDepth, filePath)
	}
	if stack[filePath] {
		return fmt.Errorf("lua include cycle detected at %s", filePath)
	}
	stack[filePath] = true
	defer delete(stack, filePath)

	data, err := fs.ReadFile(fsys, filePath)
	if err != nil {
		return fmt.Errorf("read lua %s: %w", filePath, err)
	}

	dir := path.Dir(filePath)
	matches := includeRe.FindAllSubmatchIndex(data, -1)

	cursor := 0
	for _, m := range matches {
		// m: [fullStart, fullEnd, groupStart, groupEnd]
		fullStart, fullEnd := m[0], m[1]
		ref := string(data[m[2]:m[3]])

		// Emit everything up to the directive (including the leading
		// newline if any) so original line offsets are preserved as
		// closely as the include semantics allow.
		buf.Write(data[cursor:fullStart])

		target, err := resolveInclude(dir, ref)
		if err != nil {
			return err
		}
		// 既にincludeされていてもcycleの可能性は排除しないので、
		// stackチェックをdedupより先に行う。
		if stack[target] {
			return fmt.Errorf("lua include cycle detected at %s -> %s", filePath, target)
		}
		if !included[target] {
			included[target] = true
			if err := expand(fsys, target, buf, included, stack, depth+1); err != nil {
				return err
			}
		}

		cursor = fullEnd
		// Skip the trailing newline (if any) so the directive line is
		// fully replaced, not left as a blank line. matches strip the
		// directive itself but the original line's "\n" remains in
		// data[fullEnd]; consume it if present.
		if cursor < len(data) && data[cursor] == '\n' {
			cursor++
		}
	}
	buf.Write(data[cursor:])
	return nil
}

// resolveInclude maps a BullMQ include reference to a path inside the
// embedded filesystem.
//
//   - From an entry-point script (dir == "scripts"), refs look like
//     "includes/storeJob" and resolve to "scripts/includes/storeJob.lua".
//   - From a nested include (dir == "scripts/includes"), refs look like
//     "storeJob" and resolve to "scripts/includes/storeJob.lua".
//
// References must not escape the scripts root; absolute paths and
// parent-directory traversal are rejected.
func resolveInclude(dir, ref string) (string, error) {
	if strings.HasPrefix(ref, "/") || strings.Contains(ref, "..") {
		return "", fmt.Errorf("lua include reference escapes scripts root: %q", ref)
	}
	target := path.Clean(path.Join(dir, ref)) + ".lua"
	if !strings.HasPrefix(target, scriptsRoot+"/") {
		return "", fmt.Errorf("lua include resolved outside scripts root: %q -> %q", ref, target)
	}
	return target, nil
}
