package neoql

// tx implements the sql/driver.Tx interface.
type tx struct {
	conn *conn
}

// Commit implements the Commit() method of the sql/driver.Tx interface.
// It runs a "COMMIT" Cypher query.
func (tx *tx) Commit() (err error) {
	tx.conn.tx = nil
	_, err = tx.conn.run("COMMIT", map[string]interface{}{})
	return
}

// Rollback implements the Rollback() method of the sql/driver.Tx interface.
// It runs a "ROLLBACK" Cypher query.
func (tx *tx) Rollback() (err error) {
	tx.conn.tx = nil
	_, err = tx.conn.run("ROLLBACK", map[string]interface{}{})
	return
}
