package neoql

import (
	"database/sql"
	"errors"
	"gopkg.in/neoql.v1/types"
	"strings"
	"testing"
	"time"
)

const testNeo4jURL = "bolt://neo4j:toto@0.0.0.0:7687"

func TestOpen(t *testing.T) {
	if c, err := (&neoDriver{}).Open(testNeo4jURL); err != nil {
		t.Error(err)
	} else {
		c.Close()
	}

	if c, err := (&neoDriver{}).Open(strings.Replace(testNeo4jURL, "bolt", "http", 1)); err == nil || err.Error() != "Only the 'bolt' URL scheme is supported" {
		t.Errorf("Error when scheme is invalid is not valid: %v.", err)
	} else if c != nil {
		t.Error("Connection should be nil when scheme is invalid.")
	}

	if c, err := (&neoDriver{}).Open("bolt://neo4j:invalid@0.0.0.0:7687"); c != nil {
		t.Error("Connection should be nil when authentication is invalid.")
	} else if err != types.ErrUnauthorized {
		t.Error("Error should be an UnauthorizedError.")
	}
}

type testUser struct {
	ID       uint64
	Username string
}

func (u *testUser) Scan(src interface{}) error {
	var (
		node *types.Node
		ok   bool
	)
	if node, ok = src.(*types.Node); !ok {
		return errors.New("failed to scan node")
	}
	if username, ok := node.Properties["username"]; ok {
		u.Username = username.(string)
	}
	u.ID = node.ID
	return nil
}

func TestScenario(t *testing.T) {
	var (
		err      error
		DB       *sql.DB
		stmt     *sql.Stmt
		rows     *sql.Rows
		row      *sql.Row
		tx       *sql.Tx
		node     types.Node
		node2    types.Node
		rs       types.Relationship
		path     types.Path
		username string
		user     testUser
	)
	if DB, err = sql.Open("neo4j-bolt", testNeo4jURL); err != nil {
		t.Fatal(err)
	}
	defer DB.Close()

	if err = DB.Ping(); err != nil {
		t.Error(err)
	}

	// Run basic statement
	if stmt, err = DB.Prepare("CREATE (n:User {username: {0}}) RETURN n"); err != nil {
		t.Fatal(err)
	} else if rows, err = stmt.Query("tester"); err != nil {
		t.Error(err)
	} else {
		for rows.Next() {
			if err = rows.Scan(&node); err != nil {
				t.Error(err)
				break
			}
			if node.Label != "User" {
				t.Errorf("invalid node type, expected %v got %v.", "User", node.Label)
			}
			if username, ok := node.Properties["username"]; !ok {
				t.Error("invalid node properties, missing username.")
			} else if username != "tester" {
				t.Errorf("invalid username, expected %v got %v.", "tester", node.Properties["username"])
			}
		}
		rows.Close()
	}

	// Reuse existing statement
	if rows, err = stmt.Query("tester2"); err == nil {
		for rows.Next() {
			if err = rows.Scan(&node); err != nil {
				t.Error(err)
				break
			}
			if node.Label != "User" {
				t.Errorf("invalid node type, expected %v got %v.", "User", node.Label)
			}
			if username, ok := node.Properties["username"]; !ok {
				t.Error("invalid node properties, missing username.")
			} else if username != "tester2" {
				t.Errorf("invalid username, expected %v got %v.", "tester2", node.Properties["username"])
			}
		}
		rows.Close()
	} else {
		t.Error(err)
	}
	stmt.Close()

	// DB.Query
	if rows, err = DB.Query("CREATE (n:User {username: {0}}) RETURN n", "tester3"); err == nil {
		for rows.Next() {
			if err = rows.Scan(&node); err != nil {
				t.Error(err)
				break
			}
			if node.Label != "User" {
				t.Errorf("invalid node type, expected %v got %v.", "User", node.Label)
			}
			if username, ok := node.Properties["username"]; !ok {
				t.Error("invalid node properties, missing username.")
			} else if username != "tester3" {
				t.Errorf("invalid username, expected %v got %v.", "tester3", node.Properties["username"])
			}
		}
		rows.Close()
	} else {
		t.Error(err)
	}

	// DB.QueryRow
	row = DB.QueryRow("CREATE (n:User {username: {0}}) RETURN n", "tester4")
	if err = row.Scan(&node); err != nil {
		t.Error(err)
	} else {
		if node.Label != "User" {
			t.Errorf("invalid node type, expected %v got %v.", "User", node.Label)
		}
		if username, ok := node.Properties["username"]; !ok {
			t.Error("invalid node properties, missing username.")
		} else if username != "tester4" {
			t.Errorf("invalid username, expected %v got %v.", "tester4", node.Properties["username"])
		}
	}

	// Multiple scans
	i := 0
	if rows, err = DB.Query("MATCH (n:User {username: {0}}) RETURN n, n.username", "tester5"); err == nil {
		for rows.Next() {
			if err = rows.Scan(&node, &username); err != nil {
				t.Error(err)
				break
			}
			if node.Label != "User" {
				t.Errorf("invalid node type, expected %v got %v.", "User", node.Label)
			}
			if !strings.HasPrefix(username, "tester5") {
				t.Errorf("invalid username, expected it to starts with %v, got %v.", "tester5", username)
			}
			i++
		}
		rows.Close()
	} else {
		t.Error(err)
	}

	// Custom Scan
	row = DB.QueryRow("CREATE (n:User {username: {0}}) RETURN n", "tester6")
	if err = row.Scan(&user); err != nil {
		t.Error(err)
	} else {
		if user.Username != "tester6" {
			t.Errorf("invalid username, expected %v got %v.", "tester6", user.Username)
		}
	}

	// Relationships
	row = DB.QueryRow("CREATE (n:User)-[r:RSTYPE]->(m:User) RETURN n, r, m")
	if err = row.Scan(&node, &rs, &node2); err != nil {
		t.Error(err)
	} else {
		if node.ID == 0 {
			t.Error("node ID should not equals 0.")
		}
		if node2.ID == 0 {
			t.Error("node 2 ID should not equals 0.")
		}
		if rs.FromID != node.ID {
			t.Errorf("invalid relationship StartID, expected %v got %v.", node.ID, rs.FromID)
		}
		if rs.ToID != node2.ID {
			t.Errorf("invalid relationship EndID, expected %v got %v.", node2.ID, rs.ToID)
		}
	}

	// Paths
	row = DB.QueryRow("CREATE ()-[:RSTYPE]->()-[:RSTYPE]->()-[:RSTYPE]->()")
	if err = row.Scan(); err != sql.ErrNoRows {
		t.Error(err)
	}
	row = DB.QueryRow("MATCH p = ()-[r:RSTYPE*..10]->() RETURN p")
	if err = row.Scan(&path); err != nil {
		t.Error(err)
	} else if len(path.Nodes) == 0 {
		t.Error("invalid path, should have more than zero nodes")
	} else if len(path.Relationships) == 0 {
		t.Error("invalid path, should have more than zero relationships")
	}

	// Time
	tm := types.Time{}
	now := time.Now()
	row = DB.QueryRow("CREATE (n:User {createdAt: {0}}) RETURN n.createdAt", now)
	if err = row.Scan(&tm); err != nil {
		t.Error(err)
	} else if !tm.Equal(now) {
		t.Errorf("invalid time value, expected %v got %v.", now, tm)
	}

	// Transaction
	user.ID = 0
	if tx, err = DB.Begin(); err != nil {
		t.Fatal(err)
	}
	row = tx.QueryRow("CREATE (n:User {username: {0}}) RETURN n", "tester7")
	if err = row.Scan(&user); err != nil {
		t.Error(err)
	}
	id := user.ID
	if user.ID == 0 {
		t.Error("user ID should not equals 0.")
	}
	user.ID = 0
	row = tx.QueryRow("MATCH (n) WHERE id(n) = {0} RETURN n", id)
	if err = row.Scan(&user); err != nil {
		t.Errorf("error should be nil because node should have been found, got %v.", err)
	}
	if err = tx.Rollback(); err != nil {
		t.Error(err)
	}
	// Check that is does not have been committed
	user.ID = 0
	row = DB.QueryRow("MATCH (n) WHERE id(n) = {0} RETURN n", id)
	if err = row.Scan(&user); err != sql.ErrNoRows {
		t.Errorf("error should equals %v because no result should have been found, got %v.", sql.ErrNoRows, err)
	}

	// Checks a successful commit
	user.ID = 0
	if tx, err = DB.Begin(); err != nil {
		t.Fatal(err)
	}
	row = tx.QueryRow("CREATE (n:User {username: {0}}) RETURN n", "tester8")
	if err = row.Scan(&user); err != nil {
		t.Error(err)
	}
	if user.ID == 0 {
		t.Error("user ID should not equals 0.")
	}
	if err = tx.Commit(); err != nil {
		t.Error(err)
	}
	row = DB.QueryRow("MATCH (n) WHERE id(n) = {0} RETURN n", user.ID)
	if err = row.Scan(&user); err != nil {
		t.Errorf("error should be nil because node should have been found, got %v.", err)
	}
}
