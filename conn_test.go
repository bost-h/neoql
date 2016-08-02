package neoql

import (
	"bytes"
	"database/sql/driver"
	"gopkg.in/neoql.v1/types"
	"gopkg.in/packstream.v1"
	"reflect"
	"testing"
)

// testMakeConn returns the sql/driver.Conn implementation to run tests against it.
func testMakeConn(t *testing.T) *conn {
	if c, err := (&neoDriver{}).Open(testNeo4jURL); err != nil {
		t.Fatal(err)
		return nil
	} else if cConn, ok := c.(*conn); !ok {
		t.Fatal("returned connection should be a conn.")
		return nil
	} else {
		return cConn
	}
}

// testMockConn returns the sql/driver.Conn implementation to run tests against it, and replaces the connection
// writer and reader with "rd" and "wr" for testing purposes.
func testMockConn(t *testing.T, rd *bytes.Buffer, wr *bytes.Buffer) *conn {
	c := testMakeConn(t)
	c.rd.rd = rd
	c.wr.wr = wr
	return c
}

// testGetEncodedMessage returns bytes corresponding to a packstream message sent through the chunk writer, for testing
// purposes.
func testGetEncodedMessage(t *testing.T, v interface{}) []byte {
	var b bytes.Buffer
	wr := NewWriter(&b)
	if data, err := packstream.Marshal(v); err != nil {
		t.Fatal(err)
	} else if _, err := wr.Write(data); err != nil {
		t.Fatal(err)
	} else if err := wr.Flush(true); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}

func TestNewConn(t *testing.T) {
	b := new(bytes.Buffer)
	c := testMockConn(t, b, b)
	defer c.Close()

	if c.wr == nil {
		t.Error("conn writer should not be nil.")
	} else if c.rd == nil {
		t.Error("conn reader should not be nil.")
	} else if c.enc == nil {
		t.Error("conn encoder should not be nil.")
	}
}

func TestConn_writeMessage(t *testing.T) {
	var b bytes.Buffer
	c := testMockConn(t, &b, &b)
	defer c.Close()

	decoded := packstream.Structure{Signature: 42, Fields: []interface{}{"hello", []interface{}{int64(55)}}}
	encoded := testGetEncodedMessage(t, decoded)
	if err := c.writeMessage(&decoded); err != nil {
		t.Error(err)
	} else if !bytes.Equal(b.Bytes(), encoded) {
		t.Errorf("written data are not valid, expected %# X got %# X.", b.Bytes(), encoded)
	}
}

func TestConn_readMessage(t *testing.T) {
	var b bytes.Buffer
	c := testMockConn(t, &b, &b)
	defer c.Close()

	decoded := packstream.Structure{Signature: 42, Fields: []interface{}{"hello", []interface{}{int64(55)}}}
	encoded, _ := packstream.Marshal(decoded)
	c.wr.Write(encoded)
	c.wr.Flush(true)

	if st, err := c.readMessage(); err != nil {
		t.Error(err)
	} else if !reflect.DeepEqual(st, &decoded) {
		t.Errorf("invalid structure message read, expected %v got %v.", decoded, st)
	}

	b.Reset()
	if _, err := c.readMessage(); err == nil {
		t.Error("error should not be nil when attempting to read from an empty buffer")
	} else if err != driver.ErrBadConn {
		t.Errorf("invalid error when attempting to read fron an empty buffer, expected %v, got %v.", driver.ErrBadConn, err)
	}
	c.wr.Write([]byte{0xB2})
	if _, err := c.readMessage(); err == nil {
		t.Error("error should not be nil when reading an invalid message")
	}
}

func TestConn_request(t *testing.T) {
	var b bytes.Buffer
	c := testMockConn(t, &b, &b)
	defer c.Close()

	decoded := packstream.Structure{Signature: 42, Fields: []interface{}{"hello", []interface{}{int64(55)}}}
	if st, err := c.request(&decoded); err != nil {
		t.Error(err)
	} else if !reflect.DeepEqual(st, &decoded) {
		t.Errorf("invalid structure message read, expected %v got %v.", decoded, st)
	}

}

func TestConn_auth(t *testing.T) {
	var (
		wr bytes.Buffer
		rd bytes.Buffer
	)
	c := testMockConn(t, &rd, &wr)
	defer c.Close()

	rd.Write(testGetEncodedMessage(t, packstream.NewStructure(byteSuccess)))
	data := testGetEncodedMessage(t, packstream.NewStructure(byteInit, "Neo4jBoltDriver/1.0", map[string]interface{}{
		"scheme":      "a",
		"principal":   "b",
		"credentials": "c",
	}))
	if err := c.auth("a", "b", "c"); err != nil {
		t.Error(err)
	} else if len(data) != len(wr.Bytes()) {
		t.Errorf("Unexpected authorization message, expected length of %# x got %# x.", len(data), len(wr.Bytes()))
	}
}

func TestConn_Begin(t *testing.T) {
	var (
		wr bytes.Buffer
		rd bytes.Buffer
	)
	c := testMockConn(t, &rd, &wr)
	defer c.Close()

	rd.Write(testGetEncodedMessage(t, packstream.NewStructure(byteSuccess, map[string]interface{}{"fields": []interface{}{}})))
	rd.Write(testGetEncodedMessage(t, packstream.NewStructure(byteSuccess, map[string]interface{}{})))
	data := testGetEncodedMessage(t, packstream.NewStructure(byteRun, "BEGIN", map[string]interface{}{}))
	data = append(data, testGetEncodedMessage(t, packstream.NewStructure(bytePullAll))...)
	if c.tx != nil {
		t.Errorf("transaction should be nil, got %v.", c.tx)
	} else if tx, err := c.Begin(); err != nil {
		t.Error(err)
	} else if tx == nil {
		t.Error("got an unexpected nil transaction.")
	} else if !bytes.Equal(wr.Bytes(), data) {
		t.Errorf("Unexpected transaction message, expected %# x got %# x.", data, wr.Bytes())
	} else if _, err := c.Begin(); err == nil {
		t.Error("error should not be nil when a transaction has already been opened.")
	}
}

func TestConn_Close(t *testing.T) {
	c := testMockConn(t, new(bytes.Buffer), new(bytes.Buffer))
	if err := c.Close(); err != nil {
		t.Error(err)
	} else if c.wr != nil {
		t.Errorf("connection writer should be nil, got %v", c.wr)
	} else if c.rd != nil {
		t.Errorf("connection reader should be nil, got %v", c.rd)
	} else if c.enc != nil {
		t.Errorf("connection encoder should be nil, got %v", c.enc)
	} else if c.tx != nil {
		t.Errorf("connection transaction should be nil, got %v", c.tx)
	}
}

func TestConn_Prepare(t *testing.T) {
	c := testMockConn(t, new(bytes.Buffer), new(bytes.Buffer))
	defer c.Close()

	query := "42"
	if stm, err := c.Prepare(query); err != nil {
		t.Error(err)
	} else if stm == nil {
		t.Error("stm should not be nil.")
	} else if stm, ok := stm.(*stmt); !ok {
		t.Error("stm should be a stmt.")
	} else if stm.conn != c {
		t.Errorf("unexpected connection value, expected %v got %v.", c, stm.conn)
	} else if stm.query != query {
		t.Errorf("unexpected query value, expected %v got %v.", query, stm.query)
	}
}

func TestConn_Run(t *testing.T) {
	var (
		wr bytes.Buffer
		rd bytes.Buffer
	)
	c := testMakeConn(t)
	if res, err := c.run("CREATE (n {username: {username}}) RETURN n", map[string]interface{}{"username": "tester"}); err != nil {
		t.Error(err)
	} else if res == nil {
		t.Error("statement result should not be nil.")
	} else if len(res.Fields) != 1 || res.Fields[0] != "n" {
		t.Errorf("unexpected fields response, expected %v, got %v", []string{"n"}, res.Fields)
	} else if len(res.Rows) != 1 {
		t.Errorf("expected %v rows, got %v", 1, len(res.Rows))
	} else if node, ok := res.Rows[0]["n"]; !ok {
		t.Error("row in statement result should have property n.")
	} else if node, ok := node.(*types.Node); !ok {
		t.Error("row in statement result should be a Node.")
	} else if username, ok := node.Properties["username"]; !ok {
		t.Error("node should have property username")
	} else if username != "tester" {
		t.Errorf("invalid username property, expected %v got %v", "tester", username)
	}
	c.Close()

	// Invalid server responses
	c = testMockConn(t, &rd, &wr)
	defer c.Close()
	rd.Write(testGetEncodedMessage(t, packstream.NewStructure(byteSuccess)))
	if _, err := c.run("CREATE (n {username: {username}}) RETURN n", nil); err == nil {
		t.Error("error should not be nil when message response does not have enough fields.")
	}
	rd.Write(testGetEncodedMessage(t, packstream.NewStructure(byteSuccess, []interface{}{})))
	if _, err := c.run("CREATE (n {username: {username}}) RETURN n", nil); err == nil {
		t.Error("error should not be nil when message response first field is not a map.")
	}
	rd.Write(testGetEncodedMessage(t, packstream.NewStructure(byteSuccess, map[string]interface{}{})))
	if _, err := c.run("CREATE (n {username: {username}}) RETURN n", nil); err == nil {
		t.Error("error should not be nil when message response does not contains 'fields' map key.")
	}
	rd.Write(testGetEncodedMessage(t, packstream.NewStructure(byteSuccess, map[string]interface{}{"fields": "hello"})))
	if _, err := c.run("CREATE (n {username: {username}}) RETURN n", nil); err == nil {
		t.Error("error should not be nil when message response does not contains fields are not a list.")
	}
	rd.Write(testGetEncodedMessage(t, packstream.NewStructure(byteSuccess, map[string]interface{}{"fields": []interface{}{42}})))
	if _, err := c.run("CREATE (n {username: {username}}) RETURN n", nil); err == nil {
		t.Error("error should not be nil when fields are not strings.")
	}
	rd.Write(testGetEncodedMessage(t, packstream.NewStructure(byteSuccess, map[string]interface{}{"fields": []interface{}{"testField"}})))
	rd.Write(testGetEncodedMessage(t, packstream.NewStructure(byteRecord)))
	if _, err := c.run("CREATE (n {username: {username}}) RETURN n", nil); err == nil {
		t.Error("error should not be nil when received record does not have enough fields.")
	}
	rd.Write(testGetEncodedMessage(t, packstream.NewStructure(byteSuccess, map[string]interface{}{"fields": []interface{}{"testField"}})))
	rd.Write(testGetEncodedMessage(t, packstream.NewStructure(byteRecord, 42)))
	if _, err := c.run("CREATE (n {username: {username}}) RETURN n", nil); err == nil {
		t.Error("error should not be nil when received record does not contains a list.")
	}
	rd.Write(testGetEncodedMessage(t, packstream.NewStructure(byteSuccess, map[string]interface{}{"fields": []interface{}{"testField"}})))
	rd.Write(testGetEncodedMessage(t, packstream.NewStructure(byteRecord, []interface{}{"testField", "invalid"})))
	if _, err := c.run("CREATE (n {username: {username}}) RETURN n", nil); err == nil {
		t.Error("error should not be nil when received record have too much fields.")
	}
	rd.Write(testGetEncodedMessage(t, packstream.NewStructure(byteSuccess, map[string]interface{}{"fields": []interface{}{"testField"}})))
	rd.Write(testGetEncodedMessage(t, packstream.NewStructure(byteInit)))
	if _, err := c.run("CREATE (n {username: {username}}) RETURN n", nil); err == nil {
		t.Error("error should not be nil when record response is invalid")
	}
	rd.Write(testGetEncodedMessage(t, packstream.NewStructure(byteSuccess, map[string]interface{}{"fields": []interface{}{"testField"}})))
	rd.Write(testGetEncodedMessage(t, packstream.NewStructure(byteRecord, []interface{}{"testField1"})))
	rd.Write(testGetEncodedMessage(t, packstream.NewStructure(byteRecord, []interface{}{"testField2"})))
	rd.Write(testGetEncodedMessage(t, packstream.NewStructure(byteSuccess, map[string]interface{}{})))
	if _, err := c.run("CREATE (n {username: {username}}) RETURN n", nil); err != nil {
		t.Errorf("Error should be nil on valid run response, got %v", err)
	}

	// Test summary
	rd.Write(testGetEncodedMessage(t, packstream.NewStructure(byteSuccess, map[string]interface{}{"fields": []interface{}{"testField"}})))
	rd.Write(testGetEncodedMessage(t, packstream.NewStructure(byteRecord, []interface{}{"testField1"})))
	rd.Write(testGetEncodedMessage(t, packstream.NewStructure(byteRecord, []interface{}{"testField2"})))
	rd.Write(testGetEncodedMessage(t, packstream.NewStructure(byteSuccess, nil)))
	if _, err := c.run("CREATE (n {username: {username}}) RETURN n", nil); err == nil {
		t.Error("Error should not be nil on invalid summary.")
	}
}
