package neoql

import (
	"bytes"
	"gopkg.in/packstream.v1"
	"testing"
)

func TestTx_Commit(t *testing.T) {
	var (
		wr  bytes.Buffer
		rd  bytes.Buffer
		txx *tx
	)
	c := testMockConn(t, &rd, &wr)
	txx = new(tx)
	txx.conn = c
	defer c.Close()

	rd.Write(testGetEncodedMessage(t, packstream.NewStructure(byteSuccess, map[string]interface{}{"fields": []interface{}{}})))
	rd.Write(testGetEncodedMessage(t, packstream.NewStructure(byteSuccess, map[string]interface{}{})))
	data := testGetEncodedMessage(t, packstream.NewStructure(byteRun, "COMMIT", map[string]interface{}{}))
	data = append(data, testGetEncodedMessage(t, packstream.NewStructure(bytePullAll))...)
	if err := txx.Commit(); err != nil {
		t.Error(err)
	} else if !bytes.Equal(wr.Bytes(), data) {
		t.Errorf("Unexpected commit message, expected %# x got %# x.", data, wr.Bytes())
	}
}

func TestTx_Rollback(t *testing.T) {
	var (
		wr  bytes.Buffer
		rd  bytes.Buffer
		txx *tx
	)
	c := testMockConn(t, &rd, &wr)
	txx = new(tx)
	txx.conn = c
	defer c.Close()

	rd.Write(testGetEncodedMessage(t, packstream.NewStructure(byteSuccess, map[string]interface{}{"fields": []interface{}{}})))
	rd.Write(testGetEncodedMessage(t, packstream.NewStructure(byteSuccess, map[string]interface{}{})))
	data := testGetEncodedMessage(t, packstream.NewStructure(byteRun, "ROLLBACK", map[string]interface{}{}))
	data = append(data, testGetEncodedMessage(t, packstream.NewStructure(bytePullAll))...)
	if err := txx.Rollback(); err != nil {
		t.Error(err)
	} else if !bytes.Equal(wr.Bytes(), data) {
		t.Errorf("Unexpected rollback message, expected %# x got %# x.", data, wr.Bytes())
	}
}
