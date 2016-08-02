package neoql

import (
	"database/sql/driver"
	"errors"
	"gopkg.in/neoql.v1/types"
	"gopkg.in/packstream.v1"
	"io"
)

// rows is the representation of records returned by a Cypher query.
type rows []map[string]interface{}

// result implements the sql/driver.Result interface.
type result struct {
}

// statementResult implements the sql/driver.Rows interface.
type statementResult struct {
	Fields  []string
	Rows    rows
	Type    string
	Plan    map[string]interface{}
	Profile map[string]interface{}
	cursor  int
}

// LastInsertId implements the LastInsertId() method of the sql/driver.Result interface.
// It is not supported by this driver.
func (r *result) LastInsertId() (int64, error) {
	return 0, errors.New("LastInsertId is not supported by the Neo4j driver.")
}

// RowsAffected implements the RowsAffected() method of the sql/driver.Result interface.
// It is not supported by this driver.
func (r *result) RowsAffected() (int64, error) {
	return 0, errors.New("RowsAffected is not supported.")
}

// Close implements the Close() method of the sql/driver.Rows interface.
func (r *statementResult) Close() error {
	r.Rows = nil
	r.Fields = nil
	r.Type = ""
	r.Plan = nil
	r.Profile = nil
	r.cursor = 0
	return nil
}

// Columns implements the Columns() method of the sql/driver.Rows interface.
func (r *statementResult) Columns() []string {
	return r.Fields
}

// Next implements the Next() method of the sql/driver.Rows interface.
func (r *statementResult) Next(dest []driver.Value) error {
	if r.cursor >= len(r.Rows) {
		return io.EOF
	}
	for i, f := range r.Fields {
		dest[i] = r.Rows[r.cursor][f]
	}
	r.cursor++
	return nil
}

// hydrateSummary reads a packstream structure to fill the Cypher query summary.
func (r *statementResult) hydrateSummary(st *packstream.Structure) error {
	var (
		m      map[string]interface{}
		convOK bool
	)
	if len(st.Fields) != 1 {
		return types.ErrProtocol
	}
	if m, convOK = st.Fields[0].(map[string]interface{}); !convOK {
		return types.ErrProtocol
	}
	if typ, ok := m["type"]; ok {
		if str, ok := typ.(string); ok {
			r.Type = str
		}
	}
	if plan, ok := m["plan"]; ok {
		if m, ok := plan.(map[string]interface{}); ok {
			r.Plan = m
		}
	}
	if profile, ok := m["profile"]; ok {
		if m, ok := profile.(map[string]interface{}); ok {
			r.Profile = m
		}
	}
	return nil
}
