//go:build linux

package main

import (
	"fmt"
	"os"
	"reflect"
	"testing"

	"golang.org/x/sys/unix"
)

func TestMonitorUnixKeyPollerReadsAndRestoresTerminal(t *testing.T) {
	master, err := os.OpenFile("/dev/ptmx", os.O_RDWR|unix.O_NOCTTY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer master.Close()
	if err := unix.IoctlSetPointerInt(int(master.Fd()), unix.TIOCSPTLCK, 0); err != nil {
		t.Fatal(err)
	}
	ptyNumber, err := unix.IoctlGetInt(int(master.Fd()), unix.TIOCGPTN)
	if err != nil {
		t.Fatal(err)
	}
	slave, err := os.OpenFile(fmt.Sprintf("/dev/pts/%d", ptyNumber), os.O_RDWR|unix.O_NOCTTY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer slave.Close()
	original, err := unix.IoctlGetTermios(int(slave.Fd()), unix.TCGETS)
	if err != nil {
		t.Fatal(err)
	}
	poller, err := newPlatformMonitorKeyPoller(slave)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := unix.IoctlGetTermios(int(slave.Fd()), unix.TCGETS)
	if err != nil {
		t.Fatal(err)
	}
	if raw.Lflag&(unix.ICANON|unix.ECHO) != 0 {
		t.Fatalf("terminal flags were not raw: %#x", raw.Lflag)
	}
	if _, err := master.Write([]byte{'q'}); err != nil {
		t.Fatal(err)
	}
	keys, err := poller.Poll()
	if err != nil {
		t.Fatal(err)
	}
	if string(keys) != "q" {
		t.Fatalf("polled keys = %q", keys)
	}
	if err := poller.Close(); err != nil {
		t.Fatal(err)
	}
	restored, err := unix.IoctlGetTermios(int(slave.Fd()), unix.TCGETS)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(restored, original) {
		t.Fatalf("terminal state was not restored:\noriginal=%+v\nrestored=%+v", original, restored)
	}
}
