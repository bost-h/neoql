package neoql

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"
)

const (
	// sizeMaxChunk is the maximum size of a chunk
	sizeMaxChunk = math.MaxUint16
)

var (
	// chunkZero is the representation of a zero chunk size
	chunkZero = []byte{0, 0}
)

// Writer is the implementation of the chunk writer for the Bolt protocol.
type Writer struct {
	wr io.Writer
	b  *bytes.Buffer
}

// NewWriter returns a new chunk writer
func NewWriter(wr io.Writer) *Writer {
	return &Writer{wr: wr, b: bytes.NewBuffer(make([]byte, 0, sizeMaxChunk))}
}

// Write appends the contents of p to the internal writer buffer. If this buffer exceeds the maximum chunk size, it
// flushes data as a chunk to the underlying io.Writer.
func (cw *Writer) Write(p []byte) (n int, err error) {
	var (
		size, end int
	)

	size = len(p)
	for n < size {
		if (size-n)+cw.b.Len() >= sizeMaxChunk {
			end = n + sizeMaxChunk - cw.b.Len()
			cw.b.Write(p[:end])
			if err = cw.Flush(false); err != nil {
				return n, err
			}
			n = end
		} else {
			cw.b.Write(p)
			n = size
		}
	}
	return
}

// Flush writes any buffered data to the underlying io.Writer and prepends it with the chunk size. if zeroChunk is true,
// it also writes a zero chunk size to the underlying io.Writer.
func (cw *Writer) Flush(zeroChunk bool) (err error) {
	var length int

	if length = cw.b.Len(); length > sizeMaxChunk {
		panic("Flush: Buffer.Len() > sizeMaxChunk")
	}

	if length != 0 {
		b := make([]byte, 2)
		binary.BigEndian.PutUint16(b, uint16(length))
		defer cw.b.Reset()

		if _, err = cw.wr.Write(b); err != nil {
			return
		}
		if _, err = cw.wr.Write(cw.b.Bytes()); err != nil {
			return
		}
	}
	if zeroChunk {
		if _, err = cw.wr.Write(chunkZero); err != nil {
			return
		}
	}
	return
}
