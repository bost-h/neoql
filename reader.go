package neoql

import (
	"bytes"
	"encoding/binary"
	"io"
)

// Reader is the implementation of the chunk reader for the Bolt protocol.
type Reader struct {
	rd io.Reader
}

// NewReader returns a new chunk reader.
func NewReader(rd io.Reader) *Reader {
	return &Reader{rd: rd}
}

// readChunkSize reads two bytes and compute them into an uint16 to return the chunk size.
func (r *Reader) readChunkSize() (s uint16, err error) {
	p := make([]byte, 2)
	if _, err = io.ReadFull(r.rd, p); err != nil {
		return
	}
	s = binary.BigEndian.Uint16(p)
	return
}

// ReadMessage returns a complete message by reading all chunks until it gets a zero chunk size.
func (r *Reader) ReadMessage() ([]byte, error) {
	var (
		s   uint16
		buf bytes.Buffer
		err error
	)
	for {
		if s, err = r.readChunkSize(); err != nil {
			return nil, err
		}
		if s == 0 {
			break
		}
		if _, err = io.CopyN(&buf, r.rd, int64(s)); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}
