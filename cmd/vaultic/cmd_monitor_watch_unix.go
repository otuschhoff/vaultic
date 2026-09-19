//go:build !windows

package main

import (
	"fmt"
	"io"

	"golang.org/x/sys/unix"
	"golang.org/x/term"
)

type monitorFDReader interface {
	io.Reader
	Fd() uintptr
}

type monitorUnixKeyPoller struct {
	reader monitorFDReader
	state  *term.State
}

func newPlatformMonitorKeyPoller(reader io.ReadCloser) (monitorKeyPoller, error) {
	fdReader, ok := reader.(monitorFDReader)
	if !ok {
		return nil, fmt.Errorf("monitor watch terminal input has no file descriptor")
	}
	state, err := term.MakeRaw(int(fdReader.Fd()))
	if err != nil {
		return nil, fmt.Errorf("enable monitor watch keyboard input: %w", err)
	}
	return &monitorUnixKeyPoller{reader: fdReader, state: state}, nil
}

func (poller *monitorUnixKeyPoller) Poll() ([]byte, error) {
	fd := poller.reader.Fd()
	descriptors := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}}
	count, err := unix.Poll(descriptors, 0)
	if err != nil {
		if err == unix.EINTR {
			return nil, nil
		}
		return nil, err
	}
	if count == 0 || descriptors[0].Revents&unix.POLLIN == 0 {
		return nil, nil
	}
	buffer := make([]byte, 32)
	read, readErr := poller.reader.Read(buffer)
	if read > 0 {
		return buffer[:read], nil
	}
	if readErr == io.EOF {
		return nil, nil
	}
	return nil, readErr
}

func (poller *monitorUnixKeyPoller) Close() error {
	return term.Restore(int(poller.reader.Fd()), poller.state)
}
