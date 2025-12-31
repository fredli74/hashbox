package main

import (
	"fmt"
	"os"
	"path/filepath"

	cmd "github.com/fredli74/cmdparser"
	"github.com/kardianos/osext"
)

var (
	datDirectory string
	idxDirectory string
)

func main() {
	cmd.Title = "hashbox-util"

	datDirectory, idxDirectory = defaultPaths()
	cli := newCommandSet(datDirectory, idxDirectory)

	cmd.StringOption("dat-path", "", "<path>", "Path to data directory (contains account/)", &datDirectory, cmd.Standard)
	cmd.StringOption("idx-path", "", "<path>", "Path to index directory", &idxDirectory, cmd.Standard)

	cmd.Command("list-accounts", "", func() {
		if err := cli.listAccounts(); err != nil {
			fmt.Fprintf(os.Stderr, "error listing accounts: %v\n", err)
			os.Exit(1)
		}
	})

	cmd.Command("list-datasets", "<account name>", func() {
		if len(cmd.Args) < 3 {
			fmt.Fprintln(os.Stderr, "account name required")
			os.Exit(1)
		}
		accountName := cmd.Args[2]
		if err := cli.listDatasets(accountName); err != nil {
			fmt.Fprintf(os.Stderr, "error listing datasets: %v\n", err)
			os.Exit(1)
		}
	})

	cmd.Command("list-states", "<account name> <dataset name>", func() {
		if len(cmd.Args) < 4 {
			fmt.Fprintln(os.Stderr, "account and dataset required")
			os.Exit(1)
		}
		accountName := cmd.Args[2]
		dataset := cmd.Args[3]
		if err := cli.listStates(accountName, dataset); err != nil {
			fmt.Fprintf(os.Stderr, "error listing states: %v\n", err)
			os.Exit(1)
		}
	})

	cmd.Command("rebuild-db", "", func() {
		err := cli.rebuildDB()
		if err != nil {
			fmt.Fprintf(os.Stderr, "error rebuilding db/info: %v\n", err)
			os.Exit(1)
		}
	})

	cmd.Command("show-block", "<block id>", func() {
		if len(cmd.Args) < 3 {
			fmt.Fprintln(os.Stderr, "block id required")
			os.Exit(1)
		}
		if err := cli.showBlock(cmd.Args[2]); err != nil {
			fmt.Fprintf(os.Stderr, "error showing block: %v\n", err)
			os.Exit(1)
		}
	})

	cmd.Command("", "", func() {
		fmt.Println("Commands:")
		fmt.Println("  list-accounts")
		fmt.Println("  list-datasets <account>")
		fmt.Println("  list-states <account> <dataset>")
		fmt.Println("  rebuild-db")
		fmt.Println("  show-block <block-id>")
	})

	cmd.Parse()
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
