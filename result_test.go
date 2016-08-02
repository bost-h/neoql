package neoql

import (
	"database/sql/driver"
	"gopkg.in/packstream.v1"
	"io"
	"reflect"
	"testing"
)

func TestResult_LastInsertId(t *testing.T) {
	res := new(result)
	if _, err := res.LastInsertId(); err == nil {
		t.Error("LastInsertId should return an error when it is not supported.")
	}
}

func TestResult_RowsAffected(t *testing.T) {
	res := new(result)
	if _, err := res.RowsAffected(); err == nil {
		t.Error("RowsAffected should return an error when it is not supported.")
	}
}

func TestStatementResult_Close(t *testing.T) {
	stmt := new(statementResult)
	stmt.Fields = make([]string, 0)
	stmt.Rows = make(rows, 0)
	stmt.Type = "type"
	stmt.Plan = make(map[string]interface{})
	stmt.Profile = make(map[string]interface{})
	stmt.cursor = 42

	if err := stmt.Close(); err != nil {
		t.Error(err)
	} else if stmt.Fields != nil {
		t.Errorf("statement Fields is %v, expected nil.", stmt.Fields)
	} else if stmt.Rows != nil {
		t.Errorf("statement Rows is %v, expected nil.", stmt.Rows)
	} else if stmt.Type != "" {
		t.Errorf("statement Type is %v, expected empty string.", stmt.Type)
	} else if stmt.Plan != nil {
		t.Errorf("statement Plan is %v, expected nil.", stmt.Plan)
	} else if stmt.Profile != nil {
		t.Errorf("statement Profile is %v, expected nil.", stmt.Profile)
	} else if stmt.cursor != 0 {
		t.Errorf("statement cursor is %v, expected nil.", stmt.cursor)
	}
}

func TestStatementResult_Columns(t *testing.T) {
	stmt := new(statementResult)
	stmt.Fields = []string{"field1", "field2"}

	if !reflect.DeepEqual(stmt.Columns(), stmt.Fields) {
		t.Errorf("expected columns to equal %v, got %v.", stmt.Fields, stmt.Columns())
	}
}

func TestStatementResult_Next(t *testing.T) {
	stmt := new(statementResult)
	stmt.Fields = []string{"field1", "field2"}
	stmt.Rows = rows{map[string]interface{}{"field1": 1, "field2": 2}, map[string]interface{}{"field1": 3, "field2": 4}}
	dst := make([]driver.Value, 2)

	if err := stmt.Next(dst); err != nil {
		t.Error(err)
	} else if dst[0] != 1 {
		t.Errorf("invalid value 0, got %v expected %v.", dst[0], 1)
	} else if dst[1] != 2 {
		t.Errorf("invalid value 1, got %v expected %v.", dst[1], 2)
	}

	if err := stmt.Next(dst); err != nil {
		t.Error(err)
	} else if dst[0] != 3 {
		t.Errorf("invalid value 0, got %v expected %v.", dst[0], 3)
	} else if dst[1] != 4 {
		t.Errorf("invalid value 1, got %v expected %v.", dst[1], 4)
	}

	if err := stmt.Next(dst); err == nil || err != io.EOF {
		t.Errorf("error should be EOF when there is no more rows, got %v.", err)
	}
}

func TestStatementResult_hydrateSummary(t *testing.T) {
	stmt := new(statementResult)
	tp := "The_Type"

	if err := stmt.hydrateSummary(packstream.NewStructure(0, map[string]interface{}{
		"type":    tp,
		"plan":    map[string]interface{}{"plan": 42},
		"profile": map[string]interface{}{"profile": 43}})); err != nil {
		t.Error(err)
	} else if stmt.Type != tp {
		t.Errorf("invalid type, got %v expected %v.", stmt.Type, tp)
	} else if stmt.Plan == nil {
		t.Error("invalid plan map, should not be nil.")
	} else if plan, ok := stmt.Plan["plan"]; !ok {
		t.Error("invalid plan map, should contains key 'plan'.")
	} else if plan != 42 {
		t.Errorf("invalid plan value, got %v expected %v.", plan, 42)
	} else if stmt.Profile == nil {
		t.Error("invalid profile map, should not be nil.")
	} else if profile, ok := stmt.Profile["profile"]; !ok {
		t.Error("invalid profile map, should contains key 'profile'.")
	} else if profile != 43 {
		t.Errorf("invalid profile value, got %v expected %v.", profile, 43)
	}

	// Failures
	if err := stmt.hydrateSummary(packstream.NewStructure(0)); err == nil {
		t.Error("error should not be nil when structure does not have enough fields.")
	}
	if err := stmt.hydrateSummary(packstream.NewStructure(0, 42)); err == nil {
		t.Error("error should not be nil when structure field is not a map.")
	}
	if err := stmt.hydrateSummary(packstream.NewStructure(0, 42)); err == nil {
		t.Error("error should not be nil when structure field is not a map.")
	}
	if err := stmt.hydrateSummary(packstream.NewStructure(0, map[string]interface{}{})); err != nil {
		t.Errorf("error should be nil on empty map, got %v.", err)
	}
}
