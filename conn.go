package neoql

import (
	"database/sql/driver"
	"gopkg.in/neoql.v1/types"
	"gopkg.in/packstream.v1"
	"io"
	"net"
)

const (
	byteInit       = 0x01 // Signature to initialize a connection
	byteAckFailure = 0x0F // Signature to acknowledge a failure
	byteRun        = 0x10 // Signature to run a query
	byteDiscardAll = 0x2F // Unused
	bytePullAll    = 0x3F // Signature to pull all records resulting from a query
	byteSuccess    = 0x70 // Signature to report a success
	byteRecord     = 0x71 // Signature to report a record
	byteIgnored    = 0x7E // Signature to report a response to be ignored
	byteFailure    = 0x7F // Signature to report a failure
)

// conn is the implementation of a Neo4j connection using the Bolt protocol.
type conn struct {
	wr   *Writer
	rd   *Reader
	enc  *packstream.Encoder
	conn net.Conn
	tx   *tx
}

// newConn returns a new connection, initializes its reader, writer, encoder, then attempts to authenticate to the
// Neo4j database.
func newConn(netConn net.Conn, scheme, principal, credentials string) (*conn, error) {
	c := new(conn)
	c.wr = NewWriter(netConn)
	c.rd = NewReader(netConn)
	c.enc = packstream.NewEncoder(c.wr)
	c.conn = netConn
	if err := c.auth(scheme, principal, credentials); err != nil {
		return nil, err
	}
	return c, nil
}

// writeMessage encodes a packstream structure, write it on the net.Conn then flush the chunk writer
func (c *conn) writeMessage(v *packstream.Structure) (err error) {
	if err = c.enc.Encode(v); err != nil {
		return err
	}
	err = c.wr.Flush(true)
	return err
}

// readMessage reads a packstream structure, by decoding the incoming bytes on net.Conn.
// If io.EOF or io.ErrUnexpectedEOF is received while reading, readMessage returns driver.ErrBadConn
// If the structure signature is byteFailure, it acknowledge and returns the parsed error.
func (c *conn) readMessage() (st *packstream.Structure, err error) {
	var message []byte
	if message, err = c.rd.ReadMessage(); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return nil, driver.ErrBadConn
		}
		return
	}
	if err = packstream.Unmarshal(message, &st); err != nil {
		return nil, err
	}
	if st.Signature == byteFailure {
		c.ackFailure()
		return nil, messageError(st, types.ErrProtocol)
	}
	return
}

// request calls writeMessage then readMessage and returns the read message.
func (c *conn) request(v *packstream.Structure) (*packstream.Structure, error) {
	if err := c.writeMessage(v); err != nil {
		return nil, err
	}
	return c.readMessage()
}

// auth send a message on net.Conn to authenticate using scheme, principal and credentials.
// It returns an error if authentication failed.
func (c *conn) auth(scheme, principal, credentials string) error {
	if res, err := c.request(packstream.NewStructure(byteInit, "Neo4jBoltDriver/1.0", map[string]interface{}{
		"scheme":      scheme,
		"principal":   principal,
		"credentials": credentials,
	})); err != nil {
		return err
	} else if res.Signature != byteSuccess {
		return messageError(res, types.ErrProtocol)
	}
	return nil
}

// Begin implements the Begin() method of the sql/driver.Conn interface.
// It runs a BEGIN Cypher query.
func (c *conn) Begin() (driver.Tx, error) {
	if c.tx != nil {
		return nil, ErrTransactionStarted
	}
	if _, err := c.run("BEGIN", map[string]interface{}{}); err != nil {
		return nil, err
	}
	c.tx = &tx{conn: c}
	return c.tx, nil
}

// Close implements the Close() method of the sql/driver.Conn interface.
func (c *conn) Close() error {
	c.wr = nil
	c.rd = nil
	c.enc = nil
	c.tx = nil
	return c.conn.Close()
}

// Prepare implements the Prepare() method of the sql/driver.Conn interface.
func (c *conn) Prepare(query string) (driver.Stmt, error) {
	return &stmt{conn: c, query: query}, nil
}

// run runs the "statement" Cypher query with the "params" parameters.
// Then, it pulls all records, fills the statement summary, and returns them through a statementResult.
func (c *conn) run(statement string, params map[string]interface{}) (*statementResult, error) {
	var (
		field   string
		records []interface{}
		record  interface{}
		convOK  bool
	)
	result := new(statementResult)
	if res, err := c.request(packstream.NewStructure(byteRun, statement, params)); err != nil {
		return nil, err
	} else if res.Signature != byteSuccess {
		return nil, messageError(res, types.ErrProtocol)
	} else if len(res.Fields) == 0 {
		return nil, types.ErrProtocol
	} else if m, mOk := res.Fields[0].(map[string]interface{}); !mOk {
		return nil, types.ErrProtocol
	} else if l1, l1Ok := m["fields"]; !l1Ok {
		return nil, types.ErrProtocol
	} else if l2, l2Ok := l1.([]interface{}); !l2Ok {
		return nil, types.ErrProtocol
	} else {
		result.Fields = make([]string, len(l2))
		for i := range l2 {
			if field, convOK = l2[i].(string); !convOK {
				return nil, types.ErrProtocol
			}
			result.Fields[i] = field
		}
	}

	if err := c.writeMessage(packstream.NewStructure(bytePullAll)); err != nil {
		return nil, err
	}

	result.Rows = make(rows, 0)
	i := 0
	for {
		if res, err := c.readMessage(); err != nil {
			return nil, err
		} else if res.Signature != byteSuccess && res.Signature != byteRecord {
			return nil, messageError(res, types.ErrProtocol)
		} else if len(res.Fields) == 0 {
			return nil, types.ErrProtocol
		} else if res.Signature == byteRecord {
			if records, convOK = res.Fields[0].([]interface{}); !convOK {
				return nil, types.ErrProtocol
			}
			if len(result.Fields) != len(records) {
				return nil, types.ErrProtocol
			}
			result.Rows = append(result.Rows, make(map[string]interface{}))
			j := 0
			for _, item := range records {
				if record, err = recordToType(item); err != nil {
					return nil, err
				}
				result.Rows[i][result.Fields[j]] = record
				j++
			}
			i++
		} else if res.Signature == byteSuccess {
			if err := result.hydrateSummary(res); err != nil {
				return nil, err
			}
			break
		} else {
			return nil, messageError(res, types.ErrProtocol)
		}
	}
	return result, nil
}

// ackFailure acknowledges a failure by sending the corresponding message.
// Then, it reads all messages until a "Success" message is received, and discards the "Ignored" messages.
func (c *conn) ackFailure() (err error) {
	var res *packstream.Structure

	if err = c.writeMessage(packstream.NewStructure(byteAckFailure)); err != nil {
		return
	}
	for {
		if res, err = c.readMessage(); err != nil {
			return
		}
		if res.Signature != byteIgnored {
			break
		}
	}
	if res.Signature != byteSuccess {
		err = messageError(res, types.ErrProtocol)
	}
	return
}
