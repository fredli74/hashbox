package main

import (
	"testing"

	"github.com/fredli74/hashbox/pkg/core"
)

// fakeServer answers MissingBlocks from a set of stored IDs and records each call.
type fakeServer struct {
	stored map[core.Byte128]bool
	calls  [][]core.Byte128
}

func (f *fakeServer) missingBlocks(ids []core.Byte128) []core.Byte128 {
	f.calls = append(f.calls, ids)
	var missing []core.Byte128
	for _, id := range ids {
		if !f.stored[id] {
			missing = append(missing, id)
		}
	}
	return missing
}

func resumeTestNode(name string, parent *resumeNode, contentType uint8) *resumeNode {
	return &resumeNode{entry: &FileEntry{FileName: core.String(name), ContentType: contentType, ContentBlockID: core.Hash([]byte(name))}, parent: parent}
}

func TestResolveResumeAsksOneLevelPerCall(t *testing.T) {
	top := &resumeNode{state: resumeMissing}
	photos := resumeTestNode("photos", top, ContentTypeDirectory)
	day1 := resumeTestNode("day1", photos, ContentTypeDirectory)
	a := resumeTestNode("a.jpg", day1, ContentTypeFileData)
	day2 := resumeTestNode("day2", photos, ContentTypeDirectory)
	b := resumeTestNode("b.jpg", day2, ContentTypeFileData)
	inProgress := resumeTestNode("inprogress", photos, ContentTypeDirectory)
	c := resumeTestNode("c.jpg", inProgress, ContentTypeFileData)
	d := resumeTestNode("d.jpg", inProgress, ContentTypeFileData)
	newFile := resumeTestNode("new.jpg", photos, ContentTypeFileData)
	window := []*resumeNode{photos, day1, a, day2, b, inProgress, c, d, newFile}

	server := &fakeServer{stored: map[core.Byte128]bool{}}
	for _, n := range []*resumeNode{day1, a, day2, b, c} {
		server.stored[n.entry.ContentBlockID] = true
	}
	resolveResume(window, server.missingBlocks)

	// photos, then its children, then the children of the missing inprogress folder
	if len(server.calls) != 3 || len(server.calls[0]) != 1 || len(server.calls[1]) != 4 || len(server.calls[2]) != 2 {
		t.Fatalf("calls %v, expected sizes 1, 4, 2", server.calls)
	}
	expect := map[*resumeNode]int{photos: resumeMissing, day1: resumeTrusted, a: resumeTrusted, day2: resumeTrusted, b: resumeTrusted,
		inProgress: resumeMissing, c: resumeTrusted, d: resumeMissing, newFile: resumeMissing}
	for n, state := range expect {
		if n.state != state {
			t.Errorf("%s state %d, expected %d", n.entry.FileName, n.state, state)
		}
	}
}

func TestResolveResumeTrustsUnderEarlierVerifiedFolder(t *testing.T) {
	verified := &resumeNode{state: resumeTrusted} // resolved in an earlier window
	sub := resumeTestNode("sub", verified, ContentTypeDirectory)
	f := resumeTestNode("f", sub, ContentTypeFileData)

	server := &fakeServer{stored: map[core.Byte128]bool{}}
	resolveResume([]*resumeNode{sub, f}, server.missingBlocks)
	if len(server.calls) != 0 || sub.state != resumeTrusted || f.state != resumeTrusted {
		t.Fatalf("calls %v, states %d %d, expected no calls and all trusted", server.calls, sub.state, f.state)
	}
}

func TestResolveResumeNeverAsksTrustedEntries(t *testing.T) {
	missingFolder := &resumeNode{state: resumeMissing} // resolved in an earlier window
	eod := &resumeNode{entry: &FileEntry{}, parent: missingFolder, state: resumeTrusted}
	link := resumeTestNode("link", missingFolder, ContentTypeSymLink)
	link.state = resumeTrusted // no content, as loadResumeFile marks it

	server := &fakeServer{stored: map[core.Byte128]bool{}}
	resolveResume([]*resumeNode{link, eod}, server.missingBlocks)
	if len(server.calls) != 0 {
		t.Fatalf("calls %v, expected none", server.calls)
	}
}
