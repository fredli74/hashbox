package main

import "testing"

func TestShouldIncludeAccountLevel(t *testing.T) {
	cases := []struct {
		name    string
		acc     string
		ds      string
		include []string
		exclude []string
		want    bool
	}{
		{
			name:    "include account-only",
			acc:     "root",
			ds:      "",
			include: []string{"root"},
			want:    true,
		},
		{
			name:    "include account with dataset selector",
			acc:     "root",
			ds:      "",
			include: []string{"root:dataset-alpha"},
			want:    true,
		},
		{
			name:    "include other account",
			acc:     "root",
			ds:      "",
			include: []string{"other:dataset-alpha"},
			want:    false,
		},
		{
			name:    "exclude account-only",
			acc:     "root",
			ds:      "",
			include: []string{"root"},
			exclude: []string{"root"},
			want:    false,
		},
		{
			name:    "exclude account by empty dataset selector",
			acc:     "root",
			ds:      "",
			include: []string{"root"},
			exclude: []string{"root:"},
			want:    false,
		},
		{
			name:    "exclude dataset selector does not drop account",
			acc:     "root",
			ds:      "",
			include: []string{"root"},
			exclude: []string{"root:dataset-alpha"},
			want:    true,
		},
	}

	for _, tc := range cases {
		if got := shouldInclude(tc.acc, tc.ds, tc.include, tc.exclude); got != tc.want {
			t.Fatalf("%s: got %v want %v", tc.name, got, tc.want)
		}
	}
}

func TestShouldIncludeDatasetLevel(t *testing.T) {
	cases := []struct {
		name    string
		acc     string
		ds      string
		include []string
		exclude []string
		want    bool
	}{
		{
			name:    "include exact dataset",
			acc:     "root",
			ds:      "dataset-alpha",
			include: []string{"root:dataset-alpha"},
			want:    true,
		},
		{
			name:    "exclude exact dataset",
			acc:     "root",
			ds:      "dataset-alpha",
			include: []string{"root"},
			exclude: []string{"root:dataset-alpha"},
			want:    false,
		},
		{
			name:    "include account allows other dataset",
			acc:     "root",
			ds:      "dataset-beta",
			include: []string{"root"},
			want:    true,
		},
		{
			name:    "include dataset selector blocks other dataset",
			acc:     "root",
			ds:      "dataset-beta",
			include: []string{"root:dataset-alpha"},
			want:    false,
		},
		{
			name: "no include means allow all",
			acc:  "root",
			ds:   "dataset-alpha",
			want: true,
		},
	}

	for _, tc := range cases {
		if got := shouldInclude(tc.acc, tc.ds, tc.include, tc.exclude); got != tc.want {
			t.Fatalf("%s: got %v want %v", tc.name, got, tc.want)
		}
	}
}
