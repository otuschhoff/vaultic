//go:build windows

package main

import (
	"errors"
	"fmt"
	"io"

	"golang.org/x/sys/windows"
	"golang.org/x/term"
)

type monitorWindowsFDReader interface {
	io.Reader
	Fd() uintptr
}

type monitorWindowsKeyPoller struct {
	reader monitorWindowsFDReader
	state  *term.State
}

func newPlatformMonitorKeyPoller(reader io.ReadCloser) (monitorKeyPoller, error) {
	fdReader, ok := reader.(monitorWindowsFDReader)
	if !ok {
		return nil, fmt.Errorf("monitor watch terminal input has no file descriptor")
	}
	state, err := term.MakeRaw(int(fdReader.Fd()))
	if err != nil {
		return nil, fmt.Errorf("enable monitor watch keyboard input: %w", err)
	}
	return &monitorWindowsKeyPoller{reader: fdReader, state: state}, nil
}

func (poller *monitorWindowsKeyPoller) Poll() ([]byte, error) {
	status, err := windows.WaitForSingleObject(windows.Handle(poller.reader.Fd()), 0)
	if err != nil {
		return nil, err
	}
	if status == uint32(windows.WAIT_TIMEOUT) {
		return nil, nil
	}
	if status != uint32(windows.WAIT_OBJECT_0) {
		return nil, fmt.Errorf("wait for monitor keyboard input: status %d", status)
	}
	buffer := make([]byte, 32)
	read, readErr := poller.reader.Read(buffer)
	if read > 0 {
		return buffer[:read], nil
	}
	if errors.Is(readErr, io.EOF) {
		return nil, nil
	}
	return nil, readErr
}

func (poller *monitorWindowsKeyPoller) Close() error {
	return term.Restore(int(poller.reader.Fd()), poller.state)
}
