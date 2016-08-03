/*
Package neoql is a Go Neo4j driver for the database/sql package, using the Neo4j Bolt protocol.

In most cases clients will use the database/sql package and the subpackage gopkg.in/neoql.v1/types
to benefit from Neo4J types like Nodes and Relationships.

For example:

	import (
		"database/sql"
		_ "gopkg.in/neoql.v1"
		"log"
	)

	func main() {
		var (
			err error
			db *sql.DB
		)
		if db, err = sql.Open("neo4j-bolt", "bolt://username:password@0.0.0.0:7687") ; err != nil {
			log.Fatal(err)
		}
		defer db.Close()
		...
	}

Connection string parameters

The connection string is parsed thanks to the Parse function from the url package, so you can pass username, password,
host and port in a standard way. URL scheme must be "bolt".

Neo4j version support

This driver uses the Bolt protocol, so Neo4j version 3.0 is required.

Query parameters

When running a Cypher query, you should use parameters but the placeholders must ordered numbers and then you must
respect this order when passing parameters to "Query()".

For example :
		if rows, err := db.Query("CREATE(n {username: {0}, age: {1}}) RETURN n", "Bruce", 21) ; err != nil {
			log.Fatal(err)
		}
		defer rows.Close()

To use named parameters, you may want to use another package like sqlx, it should works but have not been fully tested.

Types support

This driver supports the usual sql/driver.Value:

	int64
	float64
	bool
	[]byte
	string
	time.Time

"time.Time" is implemented using "UnixNano()" and stores the resulting int64. When time "IsZero()", it stores a zero
integer. Currently, a time.Time can only be used as a Query() parameter, it cannot be passed to Scan(). To retrieve
a time from the database, please use the Time type from the 'types' subpackage.

To use types likes Node or Relationship, see the 'types' subpackage.

Types subpackage

You will probably want to use the "types" subpackage to benefit from the Node, Relationship and Path implementations.
These types can be used as "Scan()" parameters to retrieve entities in a Neo4j way.

	Node		Can be scanned		Can't be a query parameter
	Relationship	Can be scanned		Can't be a query parameter
	Path 		Can be scanned		Can't be a query parameter
	Map 		Can be scanned		Can be a query parameter
	List 		Can be scanned		Can be a query parameter
	Time		Can be scanned		Can be a query parameter

See the code example and the "types" subpackage documentation for more information.

Code example

Here is a working example, using a node, a relationship, and a custom type User:

	import (
		"errors"
		"log"
		"database/sql"
		_ "gopkg.in/neoql.v1"
		"gopkg.in/neoql.v1/types"
		"fmt"
	)

	type User struct {
		ID uint64
		Username string
	}

	// Scanner interface implementation, so we can pass a User to Scan()
	func (user *User) Scan(src interface{}) error {
		if node, ok := src.(*types.Node); !ok {
			return errors.New("failed to scan User.")
		} else {
			user.ID = node.ID
			if username, ok := node.Properties["username"] ; ok {
				user.Username = username.(string)
			}
		}
		return nil
	}


	func main() {
		var (
			err error
			user User
			node types.Node
			rs types.Relationship
			db *sql.DB
			row *sql.Row
		)
		if db, err = sql.Open("neo4j-bolt", "bolt://neo4j:password@0.0.0.0:7687") ; err != nil {
			log.Fatal(err)
		}
		defer db.Close()

		row = db.QueryRow(`CREATE (u {username: {0}})-[r:LIKES]->(n) RETURN u, r, n`, "Bruce")
		if err = row.Scan(&user, &rs, &node) ; err != nil {
			log.Fatal(err)
		}
		fmt.Println(user, rs.ID, rs.StartID, rs.EndID, node.ID, node.Properties)
	}

*/
package neoql
