package neoql

import (
	"database/sql"
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"net/url"
)

// neoDriver is the sql/driver.Driver implementation.
type neoDriver struct {
}


// ErrTransactionStarted is returned when a user calls begin and a transaction has already been started
// on this connection.
var ErrTransactionStarted = errors.New("open: No protocol version could be agreed")

// ErrBadVersion is returned when no protocol version could be agreed with the Neo4j server.
var ErrBadVersion = errors.New("open: No protocol version could be agreed")

var (
	// magicPreamble is the required preamble to initialize the connection with a Neo4j server.
	magicPreamble = []byte{0x60, 0x60, 0xB0, 0x17}
	// versions supported and the supported Bolt protocol versions supported by the neoql driver.
	versionsSupported = [4]uint32{1, 0, 0, 0}
	// handshakeRequest is the bytes representation of the supported versions.
	handshakeRequest [16]byte
)

// init computes the supported versions into a byte slice and register the driver.
func init() {
	for i, v := range versionsSupported {
		binary.BigEndian.PutUint32(handshakeRequest[i*4:i*4+4], v)
	}

	sql.Register("neo4j-bolt", &neoDriver{})
}

// Open implements the Open() method of the sql/driver.Driver interface.
// It sends the preamble to the Neo4j server, checks the supported versions, then initializes the connection by
// authenticating to server.
func (d *neoDriver) Open(name string) (c driver.Conn, err error) {
	var (
		conn     net.Conn
		URL      *url.URL
		username string
		password string
		bVersion [4]byte
		version  uint32
	)

	if URL, err = url.Parse(name); err != nil {
		return nil, err
	}
	if URL.Scheme != "bolt" {
		return nil, errors.New("Only the 'bolt' URL scheme is supported")
	}
	if URL.User != nil {
		username = URL.User.Username()
		password, _ = URL.User.Password()
	}
	if conn, err = net.Dial("tcp", URL.Host); err != nil {
		return nil, err
	}
	if _, err := conn.Write(magicPreamble); err != nil {
		return nil, err
	}
	if _, err := conn.Write(handshakeRequest[:]); err != nil {
		return nil, err
	}
	if _, err := io.ReadFull(conn, bVersion[:]); err != nil {
		return nil, err
	}
	if version = binary.BigEndian.Uint32(bVersion[:]); version == 0 {
		return nil, ErrBadVersion
	}
	if version != 1 {
		return nil, ErrBadVersion
	}
	if c, err = newConn(conn, "basic", username, password); err != nil {
		return nil, err
	}
	return
}

/*
This type implements MarshalPS to write bytes without packstream byte slice encoding.
*/
type rawBytes []byte

// MarshalPS implements the packstream.Marshaler interface
func (b rawBytes) MarshalPS() ([]byte, error) {
	return []byte(b), nil
}
