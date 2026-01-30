package main

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"os"

	"github.com/fredli74/hashbox/pkg/accountdb"
	"github.com/fredli74/hashbox/pkg/core"
)

func resolveAccount(store *accountdb.Store, input string) (core.String, core.Byte128, error) {
	accounts, err := store.ListAccounts()
	if err != nil {
		return "", core.Byte128{}, err
	}
	if len(accounts) == 0 {
		return "", core.Byte128{}, fmt.Errorf("no accounts found")
	}

	nameInput := core.String(input)
	hash, hashCompare := parseHash(input)

	for _, acc := range accounts {
		if acc.AccountName == nameInput {
			return acc.AccountName, acc.AccountNameH, nil
		}
		if hashCompare && acc.AccountNameH.Compare(hash) == 0 {
			return acc.AccountName, acc.AccountNameH, nil
		}
	}
	return "", core.Byte128{}, fmt.Errorf("account %q not found", input)
}

// resolveDatasetName returns the canonical dataset name, accepting either the raw name
// or its hash (base64url or hex) as the input selector.
func resolveDatasetName(store *accountdb.Store, accountNameH core.Byte128, input string) (core.String, error) {
	datasets, err := store.ListDatasets(&accountNameH)
	if err != nil {
		return "", err
	}
	if len(datasets) == 0 {
		return "", fmt.Errorf("no datasets found for account")
	}

	nameInput := core.String(input)
	hash, hashCompare := parseHash(input)

	for _, ds := range datasets {
		if ds.DatasetName == nameInput {
			return ds.DatasetName, nil
		}
		// Try parsed hash (hex or base64) matches dataset name hash
		if hashCompare && hash.Compare(ds.DatasetNameH) == 0 {
			return ds.DatasetName, nil
		}
	}
	return "", fmt.Errorf("dataset %q not found", input)
}

func parseHash(input string) (core.Byte128, bool) {
	var out core.Byte128
	if decoded, err := base64.RawURLEncoding.DecodeString(input); err == nil && len(decoded) == len(out) {
		out.Set(decoded)
		return out, true
	}
	if len(input)%2 == 0 {
		if decoded, err := hex.DecodeString(input); err == nil && len(decoded) == len(out) {
			out.Set(decoded)
			return out, true
		}
	}
	return out, false
}

func formatHash(h core.Byte128) string {
	return base64.RawURLEncoding.EncodeToString(h[:])
}

func compactHumanSize(v int64) string {
	return core.CompactHumanSize(v)
}

// renameIfExists renames src to dst if src exists. Returns nil when src is
// absent, aborts on unexpected stat or rename failures.
func renameIfExists(src, dst string) {
	info, err := os.Stat(src)
	if err != nil {
		if os.IsNotExist(err) {
			return
		}
		core.Abort("stat %s: %v", src, err)
	}
	if info.IsDir() {
		core.Abort("rename on directory %s is not supported", src)
	}
	if err := os.Rename(src, dst); err != nil {
		core.Abort("rename %s -> %s: %v", src, dst, err)
	}
}
