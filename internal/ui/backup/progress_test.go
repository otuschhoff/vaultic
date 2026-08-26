package backup

import (
	"sync"
	"testing"
	"time"

	"github.com/otuschhoff/vaultic/internal/archiver"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

type mockPrinter struct {
	sync.Mutex
	vaultic.Printer
	dirUnchanged, fileNew bool
	id                    vaultic.ID
}

func (p *mockPrinter) Update(_, _ Counter, _ uint, _ map[string]struct{}, _ time.Time, _ uint64) {
}
func (p *mockPrinter) Error(_ string, err error) error        { return err }
func (p *mockPrinter) ScannerError(_ string, err error) error { return err }

func (p *mockPrinter) CompleteItem(messageType string, _ string, _ archiver.ItemStats, _ time.Duration) {
	p.Lock()
	defer p.Unlock()

	switch messageType {
	case "dir unchanged":
		p.dirUnchanged = true
	case "file new":
		p.fileNew = true
	}
}

func (p *mockPrinter) ReportTotal(_ time.Time, _ archiver.ScanStats) {}
func (p *mockPrinter) Finish(id vaultic.ID, _ *archiver.Summary, _ bool) {
	p.Lock()
	defer p.Unlock()

	p.id = id
}

func (p *mockPrinter) Reset()                {}
func (p *mockPrinter) ExcludedItem(_ string) {}

func TestProgress(t *testing.T) {
	t.Parallel()

	prnt := &mockPrinter{Printer: vaultic.NewNoopPrinter()}
	prog := newProgress(prnt, time.Millisecond)

	prog.StartFile("foo")
	prog.CompleteBlob(1024)

	// "dir unchanged"
	prog.CompleteItem("foo", archiver.ActionDirUnchanged, archiver.ItemStats{}, 0)
	// "file new"
	prog.CompleteItem("foo", archiver.ActionFileNew, archiver.ItemStats{}, 0)

	time.Sleep(10 * time.Millisecond)
	id := vaultic.NewRandomID()
	prog.Finish(id, nil, false)

	if !prnt.dirUnchanged {
		t.Error(`"dir unchanged" event not seen`)
	}
	if !prnt.fileNew {
		t.Error(`"file new" event not seen`)
	}
	if prnt.id != id {
		t.Errorf("id not stored (has %v)", prnt.id)
	}
}
