package neoql

import (
	"bytes"
	"database/sql/driver"
	"gopkg.in/packstream.v1"
	"reflect"
	"testing"
)

func TestStmt_Close(t *testing.T) {
	stm := new(stmt)
	stm.conn = new(conn)
	stm.query = "hello"

	if err := stm.Close(); err != nil {
		t.Error(err)
	} else if stm.conn != nil {
		t.Errorf("connection should be nil, got %v.", stm.conn)
	} else if stm.query != "" {
		t.Errorf("query should be empty, got %v.", stm.query)
	}
}

func TestStmt_NumInput(t *testing.T) {
	if i := new(stmt).NumInput(); i != -1 {
		t.Errorf("NumInput is not supported and should returns -1, got %v.", i)
	}
}

func TestStmt_Query(t *testing.T) {
	var (
		wr bytes.Buffer
		rd bytes.Buffer
	)
	stm := new(stmt)
	stm.conn = testMockConn(t, &rd, &wr)
	stm.query = "CREATE (n {username: {0}})"
	data := testGetEncodedMessage(t, packstream.NewStructure(byteRun, stm.query, map[string]interface{}{"0": "Bruce"}))
	data = append(data, testGetEncodedMessage(t, packstream.NewStructure(bytePullAll))...)
	rd.Write(testGetEncodedMessage(t, packstream.NewStructure(byteSuccess, map[string]interface{}{"fields": []interface{}{"testField"}})))
	rd.Write(testGetEncodedMessage(t, packstream.NewStructure(byteRecord, []interface{}{"testField1"})))
	rd.Write(testGetEncodedMessage(t, packstream.NewStructure(byteSuccess, map[string]interface{}{})))
	if _, err := stm.Query([]driver.Value{"Bruce"}); err != nil {
		t.Error(err)
	} else if !bytes.Equal(data, wr.Bytes()) {
		t.Errorf("unexpected output, expected %# x, got %# x.", data, wr.Bytes())
	}
}

func TestStmt_Exec(t *testing.T) {
	var (
		wr bytes.Buffer
		rd bytes.Buffer
	)
	stm := new(stmt)
	stm.conn = testMockConn(t, &rd, &wr)
	stm.query = "CREATE (n {username: {0}})"
	data := testGetEncodedMessage(t, packstream.NewStructure(byteRun, stm.query, map[string]interface{}{"0": "Bruce"}))
	data = append(data, testGetEncodedMessage(t, packstream.NewStructure(bytePullAll))...)
	rd.Write(testGetEncodedMessage(t, packstream.NewStructure(byteSuccess, map[string]interface{}{"fields": []interface{}{"testField"}})))
	rd.Write(testGetEncodedMessage(t, packstream.NewStructure(byteRecord, []interface{}{"testField1"})))
	rd.Write(testGetEncodedMessage(t, packstream.NewStructure(byteSuccess, map[string]interface{}{})))
	if _, err := stm.Exec([]driver.Value{"Bruce"}); err != nil {
		t.Error(err)
	} else if !bytes.Equal(data, wr.Bytes()) {
		t.Errorf("unexpected output, expected %# x, got %# x.", data, wr.Bytes())
	}
}

func TestMakeArgs(t *testing.T) {
	input := []driver.Value{42, "hello"}
	output := map[string]interface{}{"0": 42, "1": "hello"}

	if res := makeArgsMap(input); !reflect.DeepEqual(res, output) {
		t.Errorf("invalid map received, expected %v, got %v.", output, res)
	}
}
