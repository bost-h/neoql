package types

import (
	"errors"
	"fmt"
)

// CypherError represents a valid error returned by the Neo4j database.
type CypherError struct {
	Code    string // The error code returned by the Neo4j database.
	Message string // The error message returned by the Neo4j database.
}

// ErrProtocol is returned when an unexpected response is received from the Neo4j server.
var ErrProtocol = errors.New("neoql: an unsupported protocol event occurred")

// ErrUnauthorized is returned when the authentication failed.
var ErrUnauthorized = errors.New("neoql: The client is unauthorized due to authentication failure")

// Error implements the error interface.
func (e *CypherError) Error() string {
	return fmt.Sprintf("%v: %v", e.Code, e.Message)
}
