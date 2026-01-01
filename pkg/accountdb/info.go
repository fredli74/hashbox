//	 ,+---+
//	+---+´|    HASHBOX SOURCE
//	| # | |    Copyright 2015-2026
//	+---+´

package accountdb

import (
	"encoding/base64"
	"os"
	"path/filepath"
	"sort"

	"github.com/fredli74/hashbox/pkg/core"
)

type AccountListing struct {
	AccountNameH core.Byte128
	AccountName  core.String
	Datasets     core.DatasetArray
	Filename     string
}

type DatasetListing struct {
	AccountNameH core.Byte128
	DatasetName  core.String
	Filename     string
}

type AccountInfo struct {
	AccountName core.String
	AccessKey   core.Byte128
	Datasets    core.DatasetArray
}

// ReadInfoFile loads an account .info file.
func (fs *Store) ReadInfoFile(accountNameH core.Byte128) *AccountInfo {
	file, err := os.Open(fs.accountFilename(accountNameH) + ".info")
	if err != nil {
		return nil
	}
	defer file.Close()
	var a AccountInfo
	a.AccountName.Unserialize(file)
	a.AccessKey.Unserialize(file)
	a.Datasets.Unserialize(file)
	return &a
}

// WriteInfoFile writes an account .info file.
func (fs *Store) WriteInfoFile(accountNameH core.Byte128, info AccountInfo) {
	file, err := os.OpenFile(fs.accountFilename(accountNameH)+".info", os.O_RDWR|os.O_TRUNC|os.O_CREATE, 0666)
	core.AbortOn(err)
	defer file.Close()
	info.AccountName.Serialize(file)
	info.AccessKey.Serialize(file)
	info.Datasets.Serialize(file)
}

// ListAccounts returns account entries discovered via .info files.
func (fs *Store) ListAccounts() ([]AccountListing, error) {
	dir, err := os.ReadDir(filepath.Join(fs.DataDir, "account"))
	if err != nil {
		return nil, err
	}
	var out []AccountListing
	for _, de := range dir {
		name := de.Name()
		if !de.Type().IsRegular() || len(name) < len(".info") || name[len(name)-len(".info"):] != ".info" {
			continue
		}
		var h core.Byte128
		decoded, err := base64.RawURLEncoding.DecodeString(name[:len(name)-len(".info")])
		if err != nil {
			continue
		}
		h.Set(decoded)
		info := fs.ReadInfoFile(h)
		accountName := core.String("")
		var datasets core.DatasetArray
		if info != nil {
			accountName = info.AccountName
			datasets = info.Datasets
		}
		out = append(out, AccountListing{
			AccountNameH: h,
			AccountName:  accountName,
			Datasets:     datasets,
			Filename:     filepath.Join(fs.DataDir, "account", name),
		})
	}
	return out, nil
}

func (fs *Store) updateInfoFile(accountNameH core.Byte128, datasetName core.String) {
	collection := fs.ReadDBFile(accountNameH, datasetName)
	if collection == nil {
		core.Abort("updateInfoFile was called on a DB file which cannot be opened")
	}

	// Now also update account info
	info := fs.ReadInfoFile(accountNameH)
	if info == nil {
		core.Abort("unable to read account info file %s", fs.accountFilename(accountNameH)+".info")
	}
	for i := 0; i < len(info.Datasets); i++ {
		if info.Datasets[i].Name == datasetName {
			info.Datasets[i] = info.Datasets[len(info.Datasets)-1]
			info.Datasets = info.Datasets[:len(info.Datasets)-1]
			break
		}
	}
	if len(collection.States) > 0 { // Add the dataset state
		info.Datasets = append(info.Datasets, core.Dataset{Name: datasetName, Size: collection.Size, ListH: collection.ListH})
	}
	sort.Sort(info.Datasets)
	fs.WriteInfoFile(accountNameH, *info)
}

// ListDatasets returns datasets discovered via .trn files. If accountFilter is non-nil, only that account is listed.
func (fs *Store) ListDatasets(accountFilter *core.Byte128) ([]DatasetListing, error) {
	dir, err := os.ReadDir(filepath.Join(fs.DataDir, "account"))
	if err != nil {
		return nil, err
	}
	var out []DatasetListing
	for _, de := range dir {
		name := de.Name()
		matched, _ := filepath.Match("??????????????????????.??????????????????????.trn", name)
		if !matched {
			continue
		}
		var accountNameH core.Byte128
		decoded, err := base64.RawURLEncoding.DecodeString(name[:22])
		if err != nil {
			continue
		}
		accountNameH.Set(decoded)
		if accountFilter != nil && accountNameH.Compare(*accountFilter) != 0 {
			continue
		}
		datasetName := fs.GetDatasetNameFromTransactionFile(name)
		out = append(out, DatasetListing{
			AccountNameH: accountNameH,
			DatasetName:  datasetName,
			Filename:     filepath.Join(fs.DataDir, "account", name),
		})
	}
	return out, nil
}

type RebuiltDataset struct {
	AccountNameH core.Byte128
	DatasetName  core.String
	States       core.DatasetStateArray
}

// RebuildAccount clears cached info and rebuilds db/info for all datasets under the account.
func (fs *Store) RebuildAccount(accountNameH core.Byte128) {
	if info := fs.ReadInfoFile(accountNameH); info != nil {
		info.Datasets = nil
		fs.WriteInfoFile(accountNameH, *info)
	}
	datasets, err := fs.ListDatasets(&accountNameH)
	core.AbortOn(err)
	for _, d := range datasets {
		core.Log(core.LogDebug, "Regenerating file %s.db (%x.%s)", d.Filename[:45], d.AccountNameH[:], d.DatasetName)
		fs.RebuildDB(accountNameH, d.DatasetName)
	}
}
