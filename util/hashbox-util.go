package main

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"

	cmd "github.com/fredli74/cmdparser"
	"github.com/fredli74/hashbox/pkg/core"
)

var (
	datDirectory    string
	idxDirectory    string
	showDeleted     bool
	syncInclude     string
	syncExclude     string
	syncDryRun      bool
	syncQueueMB     int64 = 256
	syncThreads     int64
	logLevel        int64 = int64(core.LogInfo)
	blockInfoVerify bool
)

var Version = "(dev-build)"

func parseHostString(input string) (host string, port int) {
	host = input
	port = core.DefaultServerPort
	if idx := strings.LastIndex(input, ":"); idx != -1 {
		host = input[:idx]
		p, err := strconv.Atoi(input[idx+1:])
		core.AbortOn(err, "invalid port %q: %v", input[idx+1:], err)
		core.ASSERT(p > 0 && p < 65536, "port out of range")
		port = p
	}
	return
}

func main() {
	defer func() {
		if rec := recover(); rec != nil {
			debug.PrintStack()
			fmt.Fprintf(os.Stderr, "hashbox-util: %T %v\n", rec, rec)
			os.Exit(1)
		}
	}()
	cmd.Title = fmt.Sprintf("Hashbox Util %s", Version)
	cmd.ShowCurrentDefaults = true

	var err error
	datDirectory, err = filepath.Abs("data")
	core.AbortOn(err, "abs data dir: %v", err)
	idxDirectory, err = filepath.Abs("index")
	core.AbortOn(err, "abs index dir: %v", err)

	// Global options
	cmd.StringOption("data", "", "<path>", "Path to data directory (contains account/)", &datDirectory, cmd.Standard)
	cmd.StringOption("index", "", "<path>", "Path to index directory", &idxDirectory, cmd.Standard)
	cmd.IntOption("loglevel", "", "<level>", "Set log level (0=errors, 1=warnings, 2=info, 3=debug, 4=trace)", &logLevel, cmd.Standard).OnChange(func() {
		core.LogLevel = int(logLevel)
	})

	cmd.Command("list-accounts", "", func() {
		newCommandSet(datDirectory, idxDirectory).listAccounts()
	})

	cmd.Command("delete-account", "<account>", func() {
		if len(cmd.Args) < 3 {
			core.Abort("account required")
		}
		newCommandSet(datDirectory, idxDirectory).deleteAccount(cmd.Args[2])
	})

	cmd.Command("list-datasets", "<account>", func() {
		if len(cmd.Args) < 3 {
			core.Abort("account name required")
		}
		accountName := cmd.Args[2]
		newCommandSet(datDirectory, idxDirectory).listDatasets(accountName)
	})

	cmd.Command("move-dataset", "<src-account> <src-dataset> <dst-account> [dst-dataset]", func() {
		if len(cmd.Args) < 5 {
			core.Abort("srcAccount srcDataset dstAccount required")
		}
		dstDataset := cmd.Args[3]
		if len(cmd.Args) >= 6 {
			dstDataset = cmd.Args[5]
		}
		newCommandSet(datDirectory, idxDirectory).moveDataset(cmd.Args[2], cmd.Args[3], cmd.Args[4], dstDataset)
	})

	cmd.Command("delete-dataset", "<account> <dataset>", func() {
		if len(cmd.Args) < 4 {
			core.Abort("account and dataset required")
		}
		newCommandSet(datDirectory, idxDirectory).deleteDataset(cmd.Args[2], cmd.Args[3])
	})

	cmd.BoolOption("show-deleted", "list-states", "Include deleted states when listing", &showDeleted, cmd.Standard)
	cmd.Command("list-states", "<account> <dataset>", func() {
		if len(cmd.Args) < 4 {
			core.Abort("account and dataset required")
		}
		accountName := cmd.Args[2]
		dataset := cmd.Args[3]
		newCommandSet(datDirectory, idxDirectory).listStates(accountName, dataset)
	})

	cmd.Command("delete-state", "<account> <dataset> <state-id> [<state-id>...]", func() {
		if len(cmd.Args) < 5 {
			core.Abort("account, dataset, and at least one stateID required")
		}
		newCommandSet(datDirectory, idxDirectory).deleteStates(cmd.Args[2], cmd.Args[3], cmd.Args[4:])
	})

	cmd.Command("purge-states", "<account> <dataset>", func() {
		if len(cmd.Args) < 4 {
			core.Abort("account and dataset required")
		}
		newCommandSet(datDirectory, idxDirectory).purgeStates(cmd.Args[2], cmd.Args[3])
	})

	cmd.BoolOption("verify", "block-info", "Uncompress and verify block content", &blockInfoVerify, cmd.Standard)
	cmd.Command("block-info", "<block-id>", func() {
		if len(cmd.Args) < 3 {
			core.Abort("block id required")
		}
		newCommandSet(datDirectory, idxDirectory).showBlock(cmd.Args[2], blockInfoVerify)
	})

	cmd.Command("ping", "<host[:port]>", func() {
		if len(cmd.Args) < 3 {
			core.Abort("host required")
		}
		host, port := parseHostString(cmd.Args[2])
		newCommandSet(datDirectory, idxDirectory).ping(host, port)
	})

	cmd.Command("rebuild-db", "[account] [dataset]", func() {
		var account, dataset string
		if len(cmd.Args) >= 3 {
			account = cmd.Args[2]
		}
		if len(cmd.Args) >= 4 {
			dataset = cmd.Args[3]
		}
		newCommandSet(datDirectory, idxDirectory).rebuildDB(account, dataset)
	})

	cmd.StringOption("include", "sync", "<acct[:dataset]>[,..]", "Include patterns for sync", &syncInclude, cmd.Standard)
	cmd.StringOption("exclude", "sync", "<acct[:dataset]>[,..]", "Exclude patterns for sync", &syncExclude, cmd.Standard)
	cmd.BoolOption("dry-run", "sync", "Do not write state or apply changes", &syncDryRun, cmd.Standard)
	cmd.IntOption("queuesize", "sync", "<MiB>", "Change sending queue size", &syncQueueMB, cmd.Hidden)
	cmd.IntOption("threads", "sync", "<num>", "Change sending queue max threads", &syncThreads, cmd.Hidden)
	cmd.Command("sync", "<remote-host[:port]>", func() {
		if len(cmd.Args) < 3 {
			core.Abort("remote host required")
		}
		host, port := parseHostString(cmd.Args[2])

		include := parsePatterns(syncInclude)
		exclude := parsePatterns(syncExclude)

		cs := newCommandSet(datDirectory, idxDirectory)
		cs.queueBytes = syncQueueMB * 1024 * 1024
		if syncThreads > 0 {
			cs.maxThreads = syncThreads
		}
		cs.syncRun(host, port, include, exclude, syncDryRun)
	})

	err = cmd.Parse()
	core.AbortOn(err, "command parse failed: %v", err)
}
