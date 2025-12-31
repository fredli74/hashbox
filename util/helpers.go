package main

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strings"

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

	var targetHash core.Byte128
	var hashProvided bool
	if strings.HasPrefix(input, "#") {
		h, ok := parseHash(strings.TrimPrefix(input, "#"))
		if !ok {
			return "", core.Byte128{}, fmt.Errorf("invalid account hash %q", input)
		}
		targetHash = h
		hashProvided = true
	}
	if !hashProvided {
		if h, ok := parseHash(input); ok {
			targetHash = h
			hashProvided = true
		}
	}

	for _, acc := range accounts {
		if hashProvided {
			if acc.AccountNameH.Compare(targetHash) == 0 {
				return acc.AccountName, acc.AccountNameH, nil
			}
			continue
		}
		if acc.AccountName == core.String(input) {
			return acc.AccountName, acc.AccountNameH, nil
		}
		nameHash := core.Hash([]byte(input))
		if acc.AccountNameH.Compare(nameHash) == 0 {
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

	var hashInput core.Byte128
	hashProvided := false
	if h, ok := parseHash(input); ok {
		hashInput = h
		hashProvided = true
	}

	for _, ds := range datasets {
		if string(ds.DatasetName) == input {
			return ds.DatasetName, nil
		}
		if hashProvided {
			if hashInput.Compare(core.Hash([]byte(ds.DatasetName))) == 0 {
				return ds.DatasetName, nil
			}
		}
	}

	return "", fmt.Errorf("dataset %q not found", input)
}

// escapeControls renders control characters as \xNN to avoid terminal bell/side effects.
func escapeControls(s string) string {
	var b strings.Builder
	for _, r := range s {
		if r < 32 || r == 127 {
			fmt.Fprintf(&b, "\\x%02x", r)
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
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

func blockTypeString(t uint8) string {
	switch t {
	case core.BlockDataTypeRaw:
		return "raw"
	case core.BlockDataTypeZlib:
		return "zlib"
	default:
		return fmt.Sprintf("unknown(0x%x)", t)
	}
}
