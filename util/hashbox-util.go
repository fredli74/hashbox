package main

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime/debug"

	cmd "github.com/fredli74/cmdparser"
	"github.com/fredli74/hashbox/pkg/core"
	"github.com/kardianos/osext"
)

var (
	datDirectory string
	idxDirectory string
	showDeleted  bool
	syncInclude  string
	syncExclude  string
	syncDryRun   bool
	syncQueueMB  int64 = 256
	syncThreads  int64
	logLevel     int64 = int64(core.LogInfo)
)

func main() {
	defer func() {
		if rec := recover(); rec != nil {
			debug.PrintStack()
			fmt.Fprintf(os.Stderr, "hashbox-util: %T %v\n", rec, rec)
			os.Exit(1)
		}
	}()
	cmd.Title = "hashbox-util"

	datDirectory, idxDirectory = defaultPaths()

	// Global options
	cmd.StringOption("data", "", "<path>", "Path to data directory (contains account/)", &datDirectory, cmd.Standard)
	cmd.StringOption("index", "", "<path>", "Path to index directory", &idxDirectory, cmd.Standard)
	cmd.IntOption("loglevel", "", "<level>", "Set log level (0=errors, 1=warnings, 2=info, 3=debug, 4=trace)", &logLevel, cmd.Standard).OnChange(func() {
		core.LogLevel = int(logLevel)
	})

	cmd.Command("list-accounts", "List all accounts", func() {
		newCommandSet(datDirectory, idxDirectory).listAccounts()
	})

	cmd.Command("delete-account", "<account>  Append deletes for all datasets/states in an account", func() {
		if len(cmd.Args) < 3 {
			core.Abort("account required")
		}
		newCommandSet(datDirectory, idxDirectory).deleteAccount(cmd.Args[2])
	})

	cmd.Command("list-datasets", "<account name>  List datasets for an account", func() {
		if len(cmd.Args) < 3 {
			core.Abort("account name required")
		}
		accountName := cmd.Args[2]
		newCommandSet(datDirectory, idxDirectory).listDatasets(accountName)
	})

	cmd.Command("move-dataset", "<srcAccount> <srcDataset> <dstAccount> [dstDataset]  Merge/relocate dataset", func() {
		if len(cmd.Args) < 5 {
			core.Abort("srcAccount srcDataset dstAccount required")
		}
		dstDataset := cmd.Args[3]
		if len(cmd.Args) >= 6 {
			dstDataset = cmd.Args[5]
		}
		newCommandSet(datDirectory, idxDirectory).moveDataset(cmd.Args[2], cmd.Args[3], cmd.Args[4], dstDataset)
	})

	cmd.Command("delete-dataset", "<account> <dataset>  Append deletes for all states in a dataset", func() {
		if len(cmd.Args) < 4 {
			core.Abort("account and dataset required")
		}
		newCommandSet(datDirectory, idxDirectory).deleteDataset(cmd.Args[2], cmd.Args[3])
	})

	cmd.BoolOption("show-deleted", "list-states", "Include deleted states when listing", &showDeleted, cmd.Standard)
	cmd.Command("list-states", "<account name> <dataset name>  List states for a dataset", func() {
		if len(cmd.Args) < 4 {
			core.Abort("account and dataset required")
		}
		accountName := cmd.Args[2]
		dataset := cmd.Args[3]
		newCommandSet(datDirectory, idxDirectory).listStates(accountName, dataset)
	})

	cmd.Command("delete-state", "<account> <dataset> <stateID>  Append delete for a state", func() {
		if len(cmd.Args) < 5 {
			core.Abort("account, dataset, and stateID required")
		}
		newCommandSet(datDirectory, idxDirectory).deleteState(cmd.Args[2], cmd.Args[3], cmd.Args[4])
	})

	cmd.Command("purge-states", "<account> <dataset>  Write purged .trn with only live states", func() {
		if len(cmd.Args) < 4 {
			core.Abort("account and dataset required")
		}
		newCommandSet(datDirectory, idxDirectory).purgeStates(cmd.Args[2], cmd.Args[3])
	})

	cmd.Command("block-info", "<block id>  Show block metadata", func() {
		if len(cmd.Args) < 3 {
			core.Abort("block id required")
		}
		newCommandSet(datDirectory, idxDirectory).showBlock(cmd.Args[2])
	})

	cmd.Command("rebuild-db", "[account] [dataset]  Rebuild .db caches (all or filtered)", func() {
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
	cmd.Command("sync", "<remoteHost> <remotePort>  Sync with remote server", func() {
		if len(cmd.Args) < 4 {
			core.Abort("remoteHost and remotePort required")
		}
		include := parsePatterns(syncInclude)
		exclude := parsePatterns(syncExclude)
		port := parsePort(cmd.Args[3])
		cs := newCommandSet(datDirectory, idxDirectory)
		cs.queueBytes = syncQueueMB * 1024 * 1024
		if syncThreads > 0 {
			cs.maxThreads = syncThreads
		}
		cs.syncRun(cmd.Args[2], port, include, exclude, syncDryRun)
	})

	cmd.Command("", "", func() { cmd.Usage() })

	err := cmd.Parse()
	core.AbortOn(err, "command parse failed: %v", err)
}

// defaultPaths tries to pick a sensible default for data/index:
// 1) current working directory
// 2) $PWD
// 3) directory next to the executable
// 4) plain "data"/"index" fallback.
func defaultPaths() (string, string) {
	var bases []string
	if wd, err := os.Getwd(); err == nil && wd != "" {
		bases = append(bases, wd)
	}
	if envPwd := os.Getenv("PWD"); envPwd != "" && (len(bases) == 0 || bases[0] != envPwd) {
		bases = append(bases, envPwd)
	}
	if exeDir, err := osext.ExecutableFolder(); err == nil && exeDir != "" {
		duplicate := false
		for _, b := range bases {
			if b == exeDir {
				duplicate = true
				break
			}
		}
		if !duplicate {
			bases = append(bases, exeDir)
		}
	}
	bases = append(bases, ".")

	for _, base := range bases {
		dat := filepath.Join(base, "data")
		idx := filepath.Join(base, "index")
		if st, err := os.Stat(dat); err == nil && st.IsDir() {
			if stIdx, err := os.Stat(idx); err == nil && stIdx.IsDir() {
				return dat, idx
			}
		}
	}
	if len(bases) > 0 && bases[0] != "" {
		return filepath.Join(bases[0], "data"), filepath.Join(bases[0], "index")
	}
	return "data", "index"
}
