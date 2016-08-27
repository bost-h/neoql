package neoql

import (
	"gopkg.in/neoql.v1/types"
	"gopkg.in/packstream.v1"
	"reflect"
	"testing"
)

func TestRecordToType(t *testing.T) {
	if v, err := recordToType(42); err != nil {
		t.Error(err)
	} else if v != 42 {
		t.Errorf("invalid value, expected %v got %v.", 42, v)
	}
	m := map[string]interface{}{"test1": 1, "test2": 2}
	if v, err := recordToType(m); err != nil {
		t.Error(err)
	} else if !reflect.DeepEqual(m, v) {
		t.Errorf("invalid value, expected %v got %v.", m, v)
	}
	l := []interface{}{"test1", "test2"}
	if v, err := recordToType(l); err != nil {
		t.Error(err)
	} else if !reflect.DeepEqual(l, v) {
		t.Errorf("invalid value, expected %v got %v.", m, v)
	}
	if _, err := recordToType(*packstream.NewStructure(0)); err == nil {
		t.Error("error should not be nil when passing invalid structure")
	}
	if _, err := recordToType(*packstream.NewStructure("N"[0])); err == nil {
		t.Error("error should not be nil when passing invalid structure")
	}
}

func TestStructRecordToType(t *testing.T) {
	if _, err := recordToType(*packstream.NewStructure(0)); err == nil {
		t.Error("error should not be nil when passing invalid structure")
	}

	if v, err := recordToType(*packstream.NewStructure("N"[0], int64(42), []interface{}{"label"}, map[string]interface{}{"prop": "value"})); err != nil {
		t.Error(err)
	} else if _, ok := v.(*types.Node); !ok {
		t.Error("returned value should be a node.")
	}
}

func TestHydrateNode(t *testing.T) {
	node := new(types.Node)
	if err := hydrateNode(node, packstream.NewStructure("N"[0], int64(42), []interface{}{"label"}, map[string]interface{}{"prop": "value"})); err != nil {
		t.Error(err)
	} else if node.ID != 42 {
		t.Errorf("node has invalid ID, expected %v, got %v", 42, node.ID)
	} else if node.Label != "label" {
		t.Errorf("node has invalid label, expected %v, got %v", "label", node.Label)
	} else if prop, ok := node.Properties["prop"]; !ok {
		t.Error("node should have property prop.")
	} else if prop != "value" {
		t.Errorf("node prop has invalid value, expected %v, got %v.", "value", prop)
	}

	if err := hydrateNode(node, packstream.NewStructure("N"[0], int64(42), []interface{}{"label"})); err == nil {
		t.Error("error should not be nil when structure does not have enough fields.")
	}
	if err := hydrateNode(node, packstream.NewStructure("N"[0], "string", []interface{}{"label"}, map[string]interface{}{"prop": "value"})); err == nil {
		t.Error("error should not be nil when structure first field is not an int64.")
	}
	if err := hydrateNode(node, packstream.NewStructure("N"[0], int64(42), "label", map[string]interface{}{"prop": "value"})); err == nil {
		t.Error("error should not be nil when structure second field is not a list.")
	}
	if err := hydrateNode(node, packstream.NewStructure("N"[0], int64(42), []interface{}{42}, map[string]interface{}{"prop": "value"})); err == nil {
		t.Error("error should not be nil when structure second field is not a list of string.")
	}
	if err := hydrateNode(node, packstream.NewStructure("N"[0], int64(42), []interface{}{"label"}, 42)); err == nil {
		t.Error("error should not be nil when structure third field is not a map.")
	}
}

func TestHydrateRelationship(t *testing.T) {
	rs := new(types.Relationship)
	if err := hydrateRelationship(rs, packstream.NewStructure("R"[0], int64(1), int64(2), int64(3), "label", map[string]interface{}{"prop": "value"})); err != nil {
		t.Error(err)
	} else if rs.ID != 1 {
		t.Errorf("rs has invalid ID, expected %v, got %v", 1, rs.ID)
	} else if rs.FromID != 2 {
		t.Errorf("rs has invalid StartID, expected %v, got %v", 3, rs.FromID)
	} else if rs.ToID != 3 {
		t.Errorf("rs has invalid EndID, expected %v, got %v", 3, rs.ToID)
	} else if rs.Type != "label" {
		t.Errorf("rs has invalid type, expected %v, got %v", "label", rs.Type)
	} else if prop, ok := rs.Properties["prop"]; !ok {
		t.Error("rs should have property prop.")
	} else if prop != "value" {
		t.Errorf("rs prop has invalid value, expected %v, got %v.", "value", prop)
	}

	if err := hydrateRelationship(rs, packstream.NewStructure("R"[0], int64(2), int64(3), "label", map[string]interface{}{"prop": "value"})); err == nil {
		t.Error("error should not be nil when structure does not have enough fields.")
	}
	if err := hydrateRelationship(rs, packstream.NewStructure("R"[0], "hello", int64(2), int64(3), "label", map[string]interface{}{"prop": "value"})); err == nil {
		t.Error("error should not be nil when structure field 1 is not an int64.")
	}
	if err := hydrateRelationship(rs, packstream.NewStructure("R"[0], int64(1), "hello", int64(3), "label", map[string]interface{}{"prop": "value"})); err == nil {
		t.Error("error should not be nil when structure field 2 is not an int64.")
	}
	if err := hydrateRelationship(rs, packstream.NewStructure("R"[0], int64(1), int64(2), "hello", "label", map[string]interface{}{"prop": "value"})); err == nil {
		t.Error("error should not be nil when structure field 3 is not an int64.")
	}
	if err := hydrateRelationship(rs, packstream.NewStructure("R"[0], int64(1), int64(2), int64(3), 42, map[string]interface{}{"prop": "value"})); err == nil {
		t.Error("error should not be nil when structure field 4 is not a string.")
	}
	if err := hydrateRelationship(rs, packstream.NewStructure("R"[0], int64(1), int64(2), int64(3), "label", 42)); err == nil {
		t.Error("error should not be nil when structure field 5 is not a map.")
	}
}

func TestHydrateUnboundRelationship(t *testing.T) {
	rs := new(types.UnboundRelationship)
	if err := hydrateUnboundRelationship(rs, packstream.NewStructure("r"[0], int64(42), "label", map[string]interface{}{"prop": "value"})); err != nil {
		t.Error(err)
	} else if rs.ID != 42 {
		t.Errorf("rs has invalid ID, expected %v, got %v", 42, rs.ID)
	} else if rs.Type != "label" {
		t.Errorf("rs has invalid label, expected %v, got %v", "label", rs.Type)
	} else if prop, ok := rs.Properties["prop"]; !ok {
		t.Error("rs should have property prop.")
	} else if prop != "value" {
		t.Errorf("rs prop has invalid value, expected %v, got %v.", "value", prop)
	}

	if err := hydrateUnboundRelationship(rs, packstream.NewStructure("r"[0], int64(42), "label")); err == nil {
		t.Error("error should not be nil when structure does not have enough fields.")
	}
	if err := hydrateUnboundRelationship(rs, packstream.NewStructure("r"[0], "string", "label", map[string]interface{}{"prop": "value"})); err == nil {
		t.Error("error should not be nil when structure first field is not an int64.")
	}
	if err := hydrateUnboundRelationship(rs, packstream.NewStructure("r"[0], int64(42), 42, map[string]interface{}{"prop": "value"})); err == nil {
		t.Error("error should not be nil when structure second field is not a string.")
	}
	if err := hydrateUnboundRelationship(rs, packstream.NewStructure("r"[0], int64(42), []interface{}{"label"}, 42)); err == nil {
		t.Error("error should not be nil when structure third field is not a map.")
	}
}

func TestHydratePath(t *testing.T) {
	p := new(types.Path)
	n1 := *packstream.NewStructure("N"[0], int64(1), []interface{}{"label"}, map[string]interface{}{"prop": "value"})
	n2 := *packstream.NewStructure("N"[0], int64(2), []interface{}{"label"}, map[string]interface{}{"prop": "value"})
	n3 := *packstream.NewStructure("N"[0], int64(3), []interface{}{"label"}, map[string]interface{}{"prop": "value"})
	urs := *packstream.NewStructure("r"[0], int64(101), "label", map[string]interface{}{"prop": "value"})
	urs2 := *packstream.NewStructure("r"[0], int64(102), "label", map[string]interface{}{"prop": "value"})

	if err := hydratePath(p, packstream.NewStructure("P"[0], []interface{}{n1, n2, n3}, []interface{}{urs, urs2}, []interface{}{int64(1), int64(1), int64(2), int64(2)})); err != nil {
		t.Error(err)
	} else if len(p.Nodes) != 3 {
		t.Errorf("expected %v nodes, got %v.", 3, len(p.Nodes))
	} else if p.Nodes[0].ID != 1 {
		t.Errorf("invalid node 0, expected ID %v got %v.", 1, p.Nodes[0].ID)
	} else if p.Nodes[1].ID != 2 {
		t.Errorf("invalid node 1, expected ID %v got %v.", 2, p.Nodes[1].ID)
	} else if p.Nodes[2].ID != 3 {
		t.Errorf("invalid node 2, expected ID %v got %v.", 3, p.Nodes[2].ID)
	} else if len(p.Relationships) != 2 {
		t.Errorf("expected %v relationships, got %v.", 2, len(p.Relationships))
	} else if p.Relationships[0].FromID != 1 {
		t.Errorf("invalid relationship 0, expected StartID %v got %v.", 1, p.Relationships[0].FromID)
	} else if p.Relationships[0].From != p.Nodes[0] {
		t.Errorf("invalid relationship 0, expected Start %v got %v.", p.Nodes[0], p.Relationships[0].From)
	} else if p.Relationships[0].ToID != 2 {
		t.Errorf("invalid relationship 0, expected EndID %v got %v.", 2, p.Relationships[0].ToID)
	} else if p.Relationships[0].End != p.Nodes[1] {
		t.Errorf("invalid relationship 0, expected End %v got %v.", p.Nodes[1], p.Relationships[0].End)
	} else if p.Relationships[1].FromID != 2 {
		t.Errorf("invalid relationship 1, expected StartID %v got %v.", 2, p.Relationships[1].FromID)
	} else if p.Relationships[1].From != p.Nodes[1] {
		t.Errorf("invalid relationship 1, expected Start %v got %v.", p.Nodes[1], p.Relationships[1].From)
	} else if p.Relationships[1].ToID != 3 {
		t.Errorf("invalid relationship 1, expected EndID %v got %v.", 3, p.Relationships[1].ToID)
	} else if p.Relationships[1].End != p.Nodes[2] {
		t.Errorf("invalid relationship 1, expected End %v got %v.", p.Nodes[2], p.Relationships[1].End)
	}

	// Failures
	if err := hydratePath(p, packstream.NewStructure("P"[0], []interface{}{n1, n2, n3}, []interface{}{urs, urs2})); err == nil {
		t.Error("error should not be nil when structure does not have enough fields.")
	}
	if err := hydratePath(p, packstream.NewStructure("P"[0], "string", []interface{}{urs, urs2}, []interface{}{int64(1), int64(1), int64(2), int64(2)})); err == nil {
		t.Error("error should not be nil when structure field 1 is not a list.")
	}
	if err := hydratePath(p, packstream.NewStructure("P"[0], []interface{}{"string"}, []interface{}{urs, urs2}, []interface{}{int64(1), int64(1), int64(2), int64(2)})); err == nil {
		t.Error("error should not be nil when structure field 1 is not a list of structures.")
	}
	if err := hydratePath(p, packstream.NewStructure("P"[0], []interface{}{urs}, []interface{}{urs, urs2}, []interface{}{int64(1), int64(1), int64(2), int64(2)})); err == nil {
		t.Error("error should not be nil when structure field 1 is not a list of nodes.")
	}
	if err := hydratePath(p, packstream.NewStructure("P"[0], []interface{}{n1, n2, n3}, "string", []interface{}{int64(1), int64(1), int64(2), int64(2)})); err == nil {
		t.Error("error should not be nil when structure field 2 is not a list.")
	}
	if err := hydratePath(p, packstream.NewStructure("P"[0], []interface{}{n1, n2, n3}, []interface{}{"string"}, []interface{}{int64(1), int64(1), int64(2), int64(2)})); err == nil {
		t.Error("error should not be nil when structure field 2 is not a list of structures.")
	}
	if err := hydratePath(p, packstream.NewStructure("P"[0], []interface{}{n1, n2, n3}, []interface{}{n1}, []interface{}{int64(1), int64(1), int64(2), int64(2)})); err == nil {
		t.Error("error should not be nil when structure field 2 is not a list of relationships.")
	}
	if err := hydratePath(p, packstream.NewStructure("P"[0], []interface{}{n1, n2, n3}, []interface{}{urs, urs2}, "string")); err == nil {
		t.Error("error should not be nil when structure field 3 is not a list.")
	}
	if err := hydratePath(p, packstream.NewStructure("P"[0], []interface{}{n1, n2, n3}, []interface{}{urs, urs2}, []interface{}{"string"})); err == nil {
		t.Error("error should not be nil when structure field 3 is not a list of integers.")
	}
	if err := hydratePath(p, packstream.NewStructure("P"[0], []interface{}{n1, n2, n3}, []interface{}{urs, urs2}, []interface{}{int64(1)})); err == nil {
		t.Error("error should not be nil when structure field 3 is not a list length is not divisible by 2.")
	}
	if err := hydratePath(p, packstream.NewStructure("P"[0], []interface{}{n1, n2, n3}, []interface{}{urs, urs2}, []interface{}{int64(42), int64(1), int64(2), int64(2)})); err == nil {
		t.Error("error should not be nil when structure field 3 is not a valid sequence.")
	}
	if err := hydratePath(p, packstream.NewStructure("P"[0], []interface{}{n1, n2, n3}, []interface{}{urs, urs2}, []interface{}{int64(1), int64(42), int64(2), int64(2)})); err == nil {
		t.Error("error should not be nil when structure field 3 is not a valid sequence.")
	}
	if err := hydratePath(p, packstream.NewStructure("P"[0], []interface{}{n1, n2, n3}, []interface{}{urs, urs2}, []interface{}{int64(1), int64(1), int64(42), int64(2)})); err == nil {
		t.Error("error should not be nil when structure field 3 is not a valid sequence.")
	}
	if err := hydratePath(p, packstream.NewStructure("P"[0], []interface{}{n1, n2, n3}, []interface{}{urs, urs2}, []interface{}{int64(1), int64(1), int64(2), int64(42)})); err == nil {
		t.Error("error should not be nil when structure field 3 is not a valid sequence.")
	}
}
