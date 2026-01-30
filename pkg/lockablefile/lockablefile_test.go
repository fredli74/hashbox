//	 ,+---+
//	+---+´|    HASHBOX SOURCE
//	| # | |    Copyright 2015-2026
//	+---+´

package lockablefile

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// testDir returns a per-test directory under ./test so artifacts remain visible.
func testDir(t *testing.T, name string) string {
	t.Helper()
	base := filepath.Join(".", "test")
	if err := os.MkdirAll(base, 0o755); err != nil {
		t.Fatalf("create base test dir %q: %v", base, err)
	}
	dir := filepath.Join(base, name)
	if err := os.RemoveAll(dir); err != nil {
		t.Fatalf("cleanup test dir %q: %v", dir, err)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("create test dir %q: %v", dir, err)
	}
	return dir
}

// Helper process: holds a lock so the parent can verify cross-process blocking.
func TestLockHelperProcess(t *testing.T) {
	if os.Getenv("HASHBOX_FILELOCK_HELPER") != "1" {
		t.Skip("helper process")
	}
	path := os.Args[len(os.Args)-1]
	l, err := OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		fmt.Printf("open: %v\n", err)
		os.Exit(1)
	}
	defer l.Close()
	l.Lock()
	fmt.Println("locked") // Signal parent that the lock is held
	time.Sleep(200 * time.Millisecond)
	l.Unlock()
}

// Helper process: writes a block while holding the lock to test serialization.
func TestLockWriterHelperProcess(t *testing.T) {
	if os.Getenv("HASHBOX_FILELOCK_WRITE_HELPER") != "1" {
		t.Skip("helper process")
	}
	if len(os.Args) < 3 {
		fmt.Fprintln(os.Stderr, "missing lock/output paths")
		os.Exit(1)
	}
	label := os.Getenv("HASHBOX_FILELOCK_HELPER_LABEL")
	lockPath := os.Args[len(os.Args)-2]
	outPath := os.Args[len(os.Args)-1]

	l, err := OpenFile(lockPath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open lock: %v\n", err)
		os.Exit(1)
	}
	defer l.Close()
	l.Lock()
	defer l.Unlock()

	file, err := os.OpenFile(outPath, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open out: %v\n", err)
		os.Exit(1)
	}
	defer func() {
		if err := file.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "close out: %v\n", err)
			os.Exit(1)
		}
	}()

	if _, err := file.Seek(0, io.SeekEnd); err != nil {
		fmt.Fprintf(os.Stderr, "seek: %v\n", err)
		os.Exit(1)
	}
	for i := 0; i < 50; i++ {
		if _, err := fmt.Fprintf(file, "%s-%02d\n", label, i); err != nil {
			fmt.Fprintf(os.Stderr, "write: %v\n", err)
			os.Exit(1)
		}
		time.Sleep(2 * time.Millisecond) // encourage interleaving if lock is broken
	}
}

func TestLockBlocksAcrossProcesses(t *testing.T) {
	dir := testDir(t, "blocks-across-processes")
	lockPath := filepath.Join(dir, "lock.test")

	cmd := exec.Command(os.Args[0], "-test.run=TestLockHelperProcess", "--", lockPath)
	cmd.Env = append(os.Environ(), "HASHBOX_FILELOCK_HELPER=1")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("stdout pipe: %v", err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start helper: %v", err)
	}

	// Wait until helper holds the lock.
	scanner := bufio.NewScanner(stdout)
	if !scanner.Scan() || scanner.Text() != "locked" {
		t.Fatalf("helper did not report locking: %v", scanner.Err())
	}

	// Attempt to lock in parent; should block until helper releases.
	l, err := OpenFile(lockPath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		t.Fatalf("open in parent: %v", err)
	}
	defer l.Close()

	acquired := make(chan struct{}, 1)
	go func() {
		l.Lock()
		acquired <- struct{}{}
	}()

	select {
	case <-acquired:
		t.Fatalf("lock acquired too early, expected to block")
	case <-time.After(50 * time.Millisecond):
		// still blocked, good
	}

	// Helper should exit after releasing the lock; then we should acquire.
	if err := cmd.Wait(); err != nil {
		t.Fatalf("helper failed: %v", err)
	}

	select {
	case <-acquired:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("lock did not acquire after helper exited")
	}

	l.Unlock()
}

func TestLockOrdersConcurrentWrites(t *testing.T) {
	dir := testDir(t, "orders-concurrent-writes")
	lockPath := filepath.Join(dir, "lock.test")
	outPath := filepath.Join(dir, "output.txt")

	runWriter := func(label string) *exec.Cmd {
		cmd := exec.Command(os.Args[0], "-test.run=TestLockWriterHelperProcess", "--", lockPath, outPath)
		cmd.Env = append(os.Environ(),
			"HASHBOX_FILELOCK_WRITE_HELPER=1",
			"HASHBOX_FILELOCK_HELPER_LABEL="+label,
		)
		return cmd
	}

	first := runWriter("first")
	second := runWriter("second")

	if err := first.Start(); err != nil {
		t.Fatalf("start first writer: %v", err)
	}
	// Start the second writer almost immediately to encourage races.
	time.Sleep(10 * time.Millisecond)
	if err := second.Start(); err != nil {
		t.Fatalf("start second writer: %v", err)
	}
	if err := first.Wait(); err != nil {
		t.Fatalf("first writer failed: %v", err)
	}
	if err := second.Wait(); err != nil {
		t.Fatalf("second writer failed: %v", err)
	}

	out, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("read output: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(string(out)), "\n")

	counts := map[string]int{}
	var sequence []string
	for _, line := range lines {
		parts := strings.SplitN(line, "-", 2)
		if len(parts) != 2 {
			t.Fatalf("malformed line: %q", line)
		}
		prefix := parts[0]
		counts[prefix]++
		if len(sequence) == 0 || sequence[len(sequence)-1] != prefix {
			sequence = append(sequence, prefix)
		}
	}
	if counts["first"] != 50 || counts["second"] != 50 {
		t.Fatalf("unexpected line counts (want 50 each): %v", counts)
	}
	if len(sequence) != 2 || sequence[0] == sequence[1] {
		t.Fatalf("writes interleaved; saw label sequence %v:\n%s", sequence, string(out))
	}
}
