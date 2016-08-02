package neoql

import (
	"bytes"
	"testing"
)

func TestNewReader(t *testing.T) {
	var b bytes.Buffer
	if rd := NewReader(&b); rd == nil {
		t.Error("returned value should not be nil.")
	} else if buf, ok := rd.rd.(*bytes.Buffer); !ok {
		t.Error("reader buffer is not valid, expected a bytes buffer.")
	} else if buf != &b {
		t.Error("readerbuffer is not valid.")
	}
}

func TestReader_ReadMessage(t *testing.T) {
	var b bytes.Buffer
	rd := NewReader(&b)
	b.Write([]byte{0x00, 0x01, 42, 0x00, 0x01, 42, 0x00, 0x00})
	if p, err := rd.ReadMessage(); err != nil {
		t.Error(err)
	} else if len(p) != 2 {
		t.Errorf("invalid message received, expected length of %v got %v.", 2, len(p))
	}
}

func TestReader_readChunkSize(t *testing.T) {
	var b bytes.Buffer
	rd := NewReader(&b)
	b.Write([]byte{0x00, 42})

	if s, err := rd.readChunkSize(); err != nil {
		t.Error(err)
	} else if s != 42 {
		t.Errorf("invalid chunk length, expected %v got %v.", 42, s)
	}
}
