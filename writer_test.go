package neoql

import (
	"bytes"
	"testing"
)

func TestNewWriter(t *testing.T) {
	b := new(bytes.Buffer)
	if wr := NewWriter(b); wr == nil {
		t.Error("returned value should not be nil.")
	} else if buf, ok := wr.wr.(*bytes.Buffer); !ok {
		t.Error("buffer is not valid, expected a bytes buffer.")
	} else if buf != b {
		t.Error("buffer is not valid.")
	}
}

func TestWriter_Write(t *testing.T) {
	var b bytes.Buffer
	wr := NewWriter(&b)
	data := []byte{42}

	if i, err := wr.Write(data); err != nil {
		t.Error(err)
	} else if i != len(data) {
		t.Errorf("invalid write length, got %v expected %v.", i, len(data))
	} else if b.Len() != 0 {
		t.Errorf("writer should not have written only one byte, wrote %v.", b.Len())
	} else if i, err = wr.Write(make([]byte, sizeMaxChunk-1)); err != nil {
		t.Error(err)
	} else if i != sizeMaxChunk-1 {
		t.Errorf("invalid write length, got %v expected %v.", i, sizeMaxChunk-1)
	} else if b.Len() != sizeMaxChunk+2 {
		// +2 because of the uint16 chunk size
		t.Errorf("writer should have written %v bytes, got %v.", sizeMaxChunk+2, b.Len())
	} else if !bytes.Equal(b.Bytes()[:3], []byte{0xFF, 0xFF, 42}) {
		t.Errorf("invalid first three bytes, expected %v, got %v ", append([]byte{0xFF, 0xFF}, 42), b.Bytes()[:3])
	}
}

func TestWriter_Flush(t *testing.T) {
	b := new(bytes.Buffer)
	wr := NewWriter(b)

	wr.Write(make([]byte, sizeMaxChunk))
	if err := wr.Flush(false); err != nil {
		t.Error(err)
	} else if b.Len() != sizeMaxChunk+2 {
		t.Errorf("flush should not have written any bytes, got %v. ", b.Len()-sizeMaxChunk+2)
	} else if err = wr.Flush(true); err != nil {
		t.Error(err)
	} else if b.Len() != sizeMaxChunk+4 {
		t.Errorf("flush should not have written two zero bytes, %v bytes written. ", b.Len()-sizeMaxChunk+2)
	} else if !bytes.Equal(b.Bytes()[b.Len()-2:], chunkZero) {
		t.Errorf("invalid zero chunk written, got %v. ", b.Bytes()[b.Len()-2:])
	}
	b.Reset()
	wr.Write([]byte{42})
	if err := wr.Flush(false); err != nil {
		t.Error(err)
	} else if !bytes.Equal([]byte{0, 1, 42}, b.Bytes()) {
		t.Errorf("invalid flush, got %v expected %v", b.Bytes(), []byte{0, 1, 42})
	}

}
