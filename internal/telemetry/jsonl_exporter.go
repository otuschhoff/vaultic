package telemetry

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
)

type jsonlFile interface {
	io.Writer
	io.Seeker
	Truncate(size int64) error
	Close() error
}

type JSONLExporter struct {
	mu     sync.Mutex
	file   jsonlFile
	closed bool
}

func NewJSONLExporter(path string) (*JSONLExporter, error) {
	if path == "" {
		return nil, fmt.Errorf("monitor JSONL path is required")
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, fmt.Errorf("create monitor JSONL file: %w", err)
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("inspect monitor JSONL file: %w", err)
	}
	if !info.Mode().IsRegular() {
		_ = file.Close()
		return nil, fmt.Errorf("monitor JSONL output must be a regular file")
	}
	return &JSONLExporter{file: file}, nil
}

func (exporter *JSONLExporter) Export(ctx context.Context, snapshot MonitorSnapshot) error {
	if err := snapshot.Validate(); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	encoded, err := json.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("encode monitor JSONL snapshot: %w", err)
	}
	encoded = append(encoded, '\n')
	exporter.mu.Lock()
	defer exporter.mu.Unlock()
	if exporter.closed {
		return fmt.Errorf("monitor JSONL exporter is closed")
	}
	start, err := exporter.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return fmt.Errorf("locate monitor JSONL snapshot: %w", err)
	}
	rollback := func(writeErr error) error {
		if err := exporter.file.Truncate(start); err != nil {
			return fmt.Errorf("%w; truncate partial monitor JSONL snapshot: %v", writeErr, err)
		}
		if _, err := exporter.file.Seek(start, io.SeekStart); err != nil {
			return fmt.Errorf("%w; seek past partial monitor JSONL snapshot: %v", writeErr, err)
		}
		return writeErr
	}
	for len(encoded) != 0 {
		written, writeErr := exporter.file.Write(encoded)
		if writeErr != nil {
			return rollback(fmt.Errorf("write monitor JSONL snapshot: %w", writeErr))
		}
		if written == 0 {
			return rollback(fmt.Errorf("write monitor JSONL snapshot: zero-byte write"))
		}
		encoded = encoded[written:]
	}
	return nil
}

func (exporter *JSONLExporter) Close() error {
	if exporter == nil {
		return nil
	}
	exporter.mu.Lock()
	defer exporter.mu.Unlock()
	if exporter.closed {
		return nil
	}
	exporter.closed = true
	if err := exporter.file.Close(); err != nil {
		return fmt.Errorf("close monitor JSONL file: %w", err)
	}
	return nil
}
