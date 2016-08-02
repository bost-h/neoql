package neoql

import (
	"gopkg.in/neoql.v1/types"
	"gopkg.in/packstream.v1"
)

const (
	// unauthorizedCode is the error code returned by Neo4j when authentication failed.
	unauthorizedCode = "Neo.ClientError.Security.Unauthorized"
)

// getMessageError reads the packstream Structure, and returns an error if the message signature is a failure.
func getMessageError(res *packstream.Structure) error {
	if res.Signature != byteFailure {
		return nil
	}
	if len(res.Fields) == 0 {
		return types.ErrProtocol
	} else if m, isMap := res.Fields[0].(map[string]interface{}); !isMap {
		return types.ErrProtocol
	} else {
		err := types.CypherError{}
		if code, codeExist := m["code"]; codeExist {
			if str, ok := code.(string); ok {
				err.Code = str
			}
		}
		if message, messageExist := m["message"]; messageExist {
			if str, ok := message.(string); ok {
				err.Message = str
			}
		}
		if err.Code == "" && err.Message == "" {
			return types.ErrProtocol
		}
		if err.Code == unauthorizedCode {
			return types.ErrUnauthorized
		}
		return &err
	}
}

// messageError always returns an error by reading the "res" structure. If the structure does not contains any valid
// error, it returns the "defaultErr".
func messageError(res *packstream.Structure, defaultErr error) error {
	if err := getMessageError(res); err != nil {
		return err
	}
	return defaultErr
}
