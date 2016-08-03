/*

Package types contains types which represents Neo4j entities like Node, Relationship etc.
They should be used with the neoql driver, and most of these types implements sql/driver.Valuer of sql/driver.Scanner
interface so they can be used as parameter during calls to "Query()" or "Scan()".


	Node		Can be scanned		Can't be a query parameter
	Relationship	Can be scanned		Can't be a query parameter
	Path 		Can be scanned		Can't be a query parameter
	Map 		Can be scanned		Can be a query parameter
	List 		Can be scanned		Can be a query parameter
	Time		Can be scanned		Can be a query parameter

*/
package types

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"gopkg.in/packstream.v1"
	"time"
)

// Entity contains the common fields used by Node, UnboundRelationship and Relationship.
type Entity struct {
	ID         uint64                 // ID is the Neo4j entity ID.
	Properties map[string]interface{} // Properties contains the entity properties.
}

// Node represents a Neo4j node.
type Node struct {
	Entity
	Label string // Label is the node label
}

// Scan implements the Scanner interface, so a Node can be used as a parameter to "Scan()".
func (n *Node) Scan(src interface{}) error {
	var (
		node *Node
		ok   bool
	)

	if node, ok = src.(*Node); !ok {
		return errors.New("failed to scan node")
	}
	*n = *node
	return nil
}

// UnboundRelationship represents a Neo4j relationship with no information about its "From" or "To" nodes.
// Usually, you won't have to deal with this type.
type UnboundRelationship struct {
	Entity
	Type string // Type is the Relationship type
}

// Scan implements the Scanner interface, so a UnboundRelationship can be used as a parameter to "Scan()".
func (rs *UnboundRelationship) Scan(src interface{}) error {
	var (
		res *UnboundRelationship
		ok  bool
	)

	if res, ok = src.(*UnboundRelationship); !ok {
		return errors.New("failed to scan unbound relationship")
	}
	*rs = *res
	return nil
}

// Relationship represents a Neo4j relationship.
type Relationship struct {
	UnboundRelationship
	FromID uint64
	From   *Node
	ToID   uint64
	End    *Node
}

// Scan implements the Scanner interface, so a Relationship can be used as a parameter to "Scan()".
func (rs *Relationship) Scan(src interface{}) error {
	var (
		res *Relationship
		ok  bool
	)

	if res, ok = src.(*Relationship); !ok {
		return errors.New("failed to scan unbound relationship")
	}
	*rs = *res
	return nil
}

// Path represents a Neo4j path.
type Path struct {
	Nodes         []*Node         // Nodes are the nodes contained in the path.
	Relationships []*Relationship // Relationships are the relationships contained in the path.
}

// Scan implements the Scanner interface, so a Path can be used as a parameter to "Scan()".
func (p *Path) Scan(src interface{}) error {
	var (
		path *Path
		ok   bool
	)

	if path, ok = src.(*Path); !ok {
		return errors.New("failed to scan unbound relationship")
	}
	*p = *path
	return nil
}

// Map is a wrapper for Go maps, so it can be passed as sql/driver.Value to "Query()" calls.
type Map map[string]interface{}

// Value implements the sql/driver.Valuer.
func (m Map) Value() (driver.Value, error) {
	return packstream.Marshal(map[string]interface{}(m))
}

// Scan implements the Scanner interface, so a Map can be used as a parameter to "Scan()".
func (m Map) Scan(src interface{}) error {
	var (
		sM map[string]interface{}
		ok bool
	)
	if sM, ok = src.(map[string]interface{}); !ok {
		return errors.New("failed to scan map")
	}
	for k, v := range sM {
		m[k] = v
	}
	return nil
}

// List is a wrapper for Go slices, so it can be passed as sql/driver.Value to "Query()" calls.
type List []interface{}

// Value implements the sql/driver.Valuer.
func (l List) Value() (driver.Value, error) {
	return packstream.Marshal([]interface{}(l))
}

// Scan implements the Scanner interface, so a List can be used as a parameter to "Scan()".
// It appends the new list to the existing list.
func (l List) Scan(src interface{}) error {
	var (
		sL []interface{}
		ok bool
	)

	if sL, ok = src.([]interface{}); !ok {
		return errors.New("failed to scan list")
	} else if len(l) < len(sL) {
		return fmt.Errorf("destination list is too short, length must be %v", len(sL))
	}
	for i, v := range sL {
		l[i] = v
	}
	return nil
}



/*
Time is a wrapper for time.Time type, so it can be used despite the fact that Neo4j does have a date/time type.

It stores the number of nanoseconds elapsed since January 1, 1970 UTC, using the time.UnixNano function.
If time is a zero value, it stores a 0 int.
*/
type Time struct {
	time.Time
}

// Value implements the sql/driver.Valuer.
func (t Time) Value() (driver.Value, error) {
	return t.MarshalPS()
}

// MarshalPS implements the packstream.Marshaler interface
func (t Time) MarshalPS() ([]byte, error) {
	var i int64
	if t.IsZero() {
		i = 0
	} else {
		i = t.UnixNano()
	}
	return packstream.Marshal(i)
}

// Scan implements the Scanner interface, so a List can be used as a parameter to "Scan()".
func (t *Time) Scan(src interface{}) error {
	var (
		i  int64
		ok bool
	)
	if i, ok = src.(int64); !ok {
		return errors.New("failed to scan time")
	} else if i == 0 {
		t.Time = time.Time{}
	} else {
		t.Time = time.Unix(0, i).UTC()
	}
	return nil
}
