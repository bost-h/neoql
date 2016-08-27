package types

import (
	"bytes"
	"gopkg.in/packstream.v1"
	"testing"
	"time"
)

func TestNode_Scan(t *testing.T) {
	src := new(Node)
	dst := new(Node)
	src.ID = 1
	dst.ID = 2

	if err := dst.Scan(src); err != nil {
		t.Error(err)
	} else if dst.ID != src.ID {
		t.Errorf("destination id is %v and should be %v.", dst.ID, src.ID)
	}

	if err := dst.Scan(42); err == nil {
		t.Error("error should not be nil when passing value 42.")
	}
}

func TestRelationship_Scan(t *testing.T) {
	src := new(Relationship)
	dst := new(Relationship)
	src.ID = 1
	dst.ID = 2

	if err := dst.Scan(src); err != nil {
		t.Error(err)
	} else if dst.ID != src.ID {
		t.Errorf("destination id is %v and should be %v.", dst.ID, src.ID)
	}

	if err := dst.Scan(42); err == nil {
		t.Error("error should not be nil when passing value 42.")
	}
}

func TestUnboundRelationship_Scan(t *testing.T) {
	src := new(UnboundRelationship)
	dst := new(UnboundRelationship)
	src.ID = 1
	dst.ID = 2

	if err := dst.Scan(src); err != nil {
		t.Error(err)
	} else if dst.ID != src.ID {
		t.Errorf("destination id is %v and should be %v.", dst.ID, src.ID)
	}

	if err := dst.Scan(42); err == nil {
		t.Error("error should not be nil when passing value 42.")
	}
}

func TestPath_Scan(t *testing.T) {
	src := new(Path)
	dst := new(Path)
	src.Nodes = make([]*Node, 0)
	dst.Nodes = nil

	if err := dst.Scan(src); err != nil {
		t.Error(err)
	} else if dst.Nodes == nil {
		t.Error("destination nodes should not be nil.")
	}

	if err := dst.Scan(42); err == nil {
		t.Error("error should not be nil when passing value 42.")
	}
}

func TestMap_Scan(t *testing.T) {
	src := map[string]interface{}{"key1": "val1"}
	dst := Map{}

	if err := dst.Scan(src); err != nil {
		t.Error(err)
	} else if v, ok := dst["key1"]; !ok {
		t.Error("map key 'key1' should exists.")
	} else if v != "val1" {
		t.Errorf("unexpected value, expected %v, got %v", "val1", v)
	}

	if err := dst.Scan(42); err == nil {
		t.Error("error should not be nil when passing value 42.")
	}
}

func TestMap_Value(t *testing.T) {
	m := Map{"key1": 42}
	data, _ := packstream.Marshal(map[string]interface{}{"key1": 42})

	if v, err := m.Value(); err != nil {
		t.Error(err)
	} else if !bytes.Equal(data, v.([]byte)) {
		t.Errorf("unexpected map value, expected %# x, got %# x.", data, v)
	}
}

func TestList_Scan(t *testing.T) {
	src := []interface{}{42, "hello"}
	dst := make(List, 0)

	if err := dst.Scan(src); err != nil {
		t.Error(err)
	} else if dst[0] != 42 {
		t.Errorf("invalid list value on index 0, expected %v, got %v.", 42, dst[0])
	} else if dst[1] != "hello" {
		t.Errorf("invalid list value on index 0, expected %v, got %v.", "hello", dst[1])
	}

	if err := dst.Scan(42); err == nil {
		t.Error("error should not be nil when passing value 42.")
	}}

func TestList_Value(t *testing.T) {
	l := List{"val1", 42}
	data, _ := packstream.Marshal([]interface{}{"val1", 42})

	if v, err := l.Value(); err != nil {
		t.Error(err)
	} else if !bytes.Equal(data, v.([]byte)) {
		t.Errorf("unexpected list value, expected %# x, got %# x.", data, v)
	}
}

func TestTime_MarshalPS(t *testing.T) {
	now := time.Now()
	tm := Time{now}
	i := now.UnixNano()
	data, _ := packstream.Marshal(i)

	if encoded, err := tm.MarshalPS(); err != nil {
		t.Error(err)
	} else if !bytes.Equal(encoded, data) {
		t.Errorf("unexpected marshaled value, got %# x expected %# x.", encoded, data)
	}

	tm = Time{time.Time{}}
	data, _ = packstream.Marshal(0)
	if encoded, err := tm.MarshalPS(); err != nil {
		t.Error(err)
	} else if !bytes.Equal(encoded, data) {
		t.Errorf("unexpected marshaled value, got %# x expected %# x.", encoded, data)
	}
}

func TestTime_Value(t *testing.T) {
	now := time.Now()
	tm := Time{now}
	i := now.UnixNano()
	data, _ := packstream.Marshal(i)

	if encoded, err := tm.Value(); err != nil {
		t.Error(err)
	} else if !bytes.Equal(encoded.([]byte), data) {
		t.Errorf("unexpected marshaled value, got %# x expected %# x.", encoded, data)
	}

	tm = Time{time.Time{}}
	data, _ = packstream.Marshal(0)
	if encoded, err := tm.Value(); err != nil {
		t.Error(err)
	} else if !bytes.Equal(encoded.([]byte), data) {
		t.Errorf("unexpected marshaled value, got %# x expected %# x.", encoded, data)
	}
}

func TestTime_Scan(t *testing.T) {
	now := time.Now()
	i := now.UnixNano()
	tm := Time{}

	if err := tm.Scan(i); err != nil {
		t.Error(err)
	} else if !tm.Equal(now) {
		t.Errorf("unexpected time value, got %v expected %v.", tm, now)
	}

	if err := tm.Scan(int64(0)); err != nil {
		t.Error(err)
	} else if !tm.IsZero() {
		t.Errorf("unexpected time value, got %v expected zero time.", tm)
	}
}
