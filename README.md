# Neoql


[![Build Status](https://travis-ci.org/go-neoql/neoql.svg?branch=v1)](https://travis-ci.org/go-neoql/neoql)

Neoql is a Go database/sql implementation for Neo4j databases, using the
new Bolt protocol.

## Install

    import "gopkg.in/neoql.v1"

## Documentation

[![GoDoc](https://godoc.org/gopkg.in/neoql.v1?status.svg)](https://godoc.org/gopkg.in/neoql.v1)

For detailed documentation, please see the package documentation at https://godoc.org/gopkg.in/neoql.v1.

## Version support

Because the purpose of neoql is to use the new Bolt protocol, Neo4j v3.0 (or newer) is required.
If you have an older version, you may want to use the [cq driver](https://github.com/go-cq/cq)

Furthermore, this package is tested against Go version 1.5 and 1.6.

## Example

This example creates two nodes, one relationship, and use a custom type which implements the Scanner interface.
For more information, please refer to the [documentation](https://godoc.org/gopkg.in/neoql.v1), and feel free to use the Github
issue system to ask a question.

```go
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
```