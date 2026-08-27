package schema

import (
	"encoding/binary"
	"fmt"
	"math"
)

const (
	MaxEncodedValueBytes = 15 * 1024 * 1024
	maxFieldBytes        = MaxEncodedValueBytes
)

type encoder struct{ data []byte }

func newEncoder() *encoder       { return &encoder{data: []byte{Version}} }
func (e *encoder) u8(value byte) { e.data = append(e.data, value) }
func (e *encoder) bool(value bool) {
	if value {
		e.u8(1)
	} else {
		e.u8(0)
	}
}
func (e *encoder) u32(value uint32) {
	e.data = binary.BigEndian.AppendUint32(e.data, value)
}
func (e *encoder) u64(value uint64) {
	e.data = binary.BigEndian.AppendUint64(e.data, value)
}
func (e *encoder) i64(value int64) { e.u64(uint64(value)) }
func (e *encoder) id(value ID)     { e.data = append(e.data, value[:]...) }
func (e *encoder) bytes(value []byte) error {
	if len(value) > maxFieldBytes || len(value) > math.MaxUint32 {
		return fmt.Errorf("%w: field is too large", ErrMalformed)
	}
	e.u32(uint32(len(value)))
	e.data = append(e.data, value...)
	return nil
}
func (e *encoder) string(value string) error { return e.bytes([]byte(value)) }
func (e *encoder) finish() ([]byte, error) {
	if len(e.data) > MaxEncodedValueBytes {
		return nil, fmt.Errorf("%w: encoded value is too large", ErrMalformed)
	}
	return e.data, nil
}

type decoder struct {
	data []byte
	at   int
}

func newDecoder(data []byte) (*decoder, error) {
	if len(data) == 0 || len(data) > MaxEncodedValueBytes || data[0] != Version {
		return nil, fmt.Errorf("%w: unsupported or missing schema version", ErrMalformed)
	}
	return &decoder{data: data, at: 1}, nil
}
func (d *decoder) take(size int) ([]byte, error) {
	if size < 0 || d.at > len(d.data)-size {
		return nil, fmt.Errorf("%w: truncated value", ErrMalformed)
	}
	value := d.data[d.at : d.at+size]
	d.at += size
	return value, nil
}
func (d *decoder) u8() (byte, error) {
	value, err := d.take(1)
	if err != nil {
		return 0, err
	}
	return value[0], nil
}
func (d *decoder) bool() (bool, error) {
	value, err := d.u8()
	if err != nil {
		return false, err
	}
	if value > 1 {
		return false, fmt.Errorf("%w: invalid boolean", ErrMalformed)
	}
	return value == 1, nil
}
func (d *decoder) u32() (uint32, error) {
	value, err := d.take(4)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(value), nil
}
func (d *decoder) u64() (uint64, error) {
	value, err := d.take(8)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(value), nil
}
func (d *decoder) i64() (int64, error) {
	value, err := d.u64()
	return int64(value), err
}
func (d *decoder) id() (ID, error) {
	var id ID
	value, err := d.take(len(id))
	if err != nil {
		return id, err
	}
	copy(id[:], value)
	return id, nil
}
func (d *decoder) bytes() ([]byte, error) {
	size, err := d.u32()
	if err != nil {
		return nil, err
	}
	if size > maxFieldBytes {
		return nil, fmt.Errorf("%w: field is too large", ErrMalformed)
	}
	value, err := d.take(int(size))
	if err != nil {
		return nil, err
	}
	return append([]byte(nil), value...), nil
}
func (d *decoder) string() (string, error) {
	value, err := d.bytes()
	return string(value), err
}
func (d *decoder) done() error {
	if d.at != len(d.data) {
		return fmt.Errorf("%w: trailing data", ErrMalformed)
	}
	return nil
}

func marshalNextRevision(next uint64) []byte {
	result := []byte{Version}
	return binary.BigEndian.AppendUint64(result, next)
}

func UnmarshalNextRevision(data []byte) (uint64, error) {
	decoder, err := newDecoder(data)
	if err != nil {
		return 0, err
	}
	value, err := decoder.u64()
	if err != nil {
		return 0, err
	}
	if value == 0 {
		return 0, fmt.Errorf("%w: next revision must be non-zero", ErrMalformed)
	}
	return value, decoder.done()
}
