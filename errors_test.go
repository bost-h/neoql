package neoql

import (
	"errors"
	"gopkg.in/neoql.v0/types"
	"gopkg.in/packstream.v1"
	"testing"
)

func TestGetMessageError(t *testing.T) {
	if err := getMessageError(packstream.NewStructure(byteSuccess)); err != nil {
		t.Error(err)
	}

	if err := getMessageError(packstream.NewStructure(byteFailure)); err == nil {
		t.Error("error should not be nil when byte failure is set and there is no other fields.")
	} else if err != types.ErrProtocol {
		t.Error("error should be a ProtocolError when byte failure is set and there is no other fields.")
	}
	if err := getMessageError(packstream.NewStructure(byteFailure, 42)); err == nil {
		t.Error("error should not be nil when byte failure is set and second field is not a map.")
	} else if err != types.ErrProtocol {
		t.Error("error should be a ProtocolError when byte failure is set and second field is not a map.")
	}
	if err := getMessageError(packstream.NewStructure(byteFailure, map[string]interface{}{})); err == nil {
		t.Error("error should not be nil when byte failure is set and second field is an empty map.")
	} else if err != types.ErrProtocol {
		t.Error("error should be a ProtocolError when byte failure is set and second field is an empty map.")
	}
	if err := getMessageError(packstream.NewStructure(byteFailure, map[string]interface{}{"code": 4})); err == nil {
		t.Error("error should not be nil when code is an integer.")
	} else if err != types.ErrProtocol {
		t.Error("error should be a ProtocolError when code is an integer.")
	}
	if err := getMessageError(packstream.NewStructure(byteFailure, map[string]interface{}{"code": "string"})); err == nil {
		t.Error("error should not be nil when code is valid.")
	} else if cErr, ok := err.(*types.CypherError); !ok {
		t.Error("error should be a CypherError when code is valid.")
	} else if cErr.Code != "string" {
		t.Errorf("error code should be %v, got %v", "string", cErr.Code)
	}
	if err := getMessageError(packstream.NewStructure(byteFailure, map[string]interface{}{"code": "string", "message": 42})); err == nil {
		t.Error("error should not be nil when code is valid and message is an int.")
	} else if cErr, ok := err.(*types.CypherError); !ok {
		t.Error("error should be a CypherError when code is valid and message is an int.")
	} else if cErr.Code != "string" {
		t.Errorf("error code should be %v, got %v", "string", cErr.Code)
	}
	if err := getMessageError(packstream.NewStructure(byteFailure, map[string]interface{}{"message": 42})); err == nil {
		t.Error("error should not be nil when message is an int.")
	} else if err != types.ErrProtocol {
		t.Error("error should be a ProtocolError when message is an int.")
	}
	if err := getMessageError(packstream.NewStructure(byteFailure, map[string]interface{}{"message": "string"})); err == nil {
		t.Error("error should not be nil when message is valid.")
	} else if cErr, ok := err.(*types.CypherError); !ok {
		t.Error("error should be a CypherError when message is valid.")
	} else if cErr.Message != "string" {
		t.Errorf("error message should be %v, got %v", "string", cErr.Code)
	}
	if err := getMessageError(packstream.NewStructure(byteFailure, map[string]interface{}{"code": "42", "message": "hello"})); err == nil {
		t.Error("error should not be nil when message and code are valid.")
	} else if cErr, ok := err.(*types.CypherError); !ok {
		t.Error("error should be a CypherError when message and core are valid.")
	} else if cErr.Message != "hello" {
		t.Errorf("error message should be %v, got %v", "hello", cErr.Code)
	} else if cErr.Code != "42" {
		t.Errorf("error code should be %v, got %v", "42", cErr.Code)
	}
	if err := getMessageError(packstream.NewStructure(byteFailure, map[string]interface{}{"code": unauthorizedCode, "message": "hello"})); err == nil {
		t.Error("error should not be nil when error is an valid UnauthorizedError.")
	} else if err != types.ErrUnauthorized {
		t.Error("error should be an UnauthorizedError when error is valid .")
	}
}

func TestMessageError(t *testing.T) {
	if err := messageError(packstream.NewStructure(byteSuccess), errors.New("default")); err == nil {
		t.Error("error should not be nil when calling message error")
	} else if err.Error() != "default" {
		t.Error("error should equals the second parameter when structure signature is byteSuccess.")
	}
	if err := messageError(packstream.NewStructure(byteFailure), errors.New("default")); err == nil {
		t.Error("error should not be nil when byte failure is set.")
	} else if err != types.ErrProtocol {
		t.Error("error should be a ProtocolError when byte failure is set and there is no other fields.")
	}
}
