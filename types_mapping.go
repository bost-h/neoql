package neoql

import (
	"gopkg.in/neoql.v0/types"
	"gopkg.in/packstream.v1"
)

// hydrateNode reads a packstream structure and hydrate a Node with its data.
// If the structure does not represent a valid Node, it returns a types.ProtocolError.
func hydrateNode(n *types.Node, st *packstream.Structure) error {
	var (
		ID        int64
		labelList []interface{}
		label     string
		props     map[string]interface{}
		convOK    bool
	)

	if len(st.Fields) != 3 {
		return types.ErrProtocol
	}

	// ID
	if ID, convOK = st.Fields[0].(int64); !convOK {
		return types.ErrProtocol
	}
	n.ID = uint64(ID)

	// Label
	if labelList, convOK = st.Fields[1].([]interface{}); !convOK {
		return types.ErrProtocol
	}
	if len(labelList) > 0 {
		if label, convOK = labelList[0].(string); !convOK {
			return types.ErrProtocol
		}
		n.Label = label
	}

	// Properties
	if props, convOK = st.Fields[2].(map[string]interface{}); !convOK {
		return types.ErrProtocol
	}
	n.Properties = props
	return nil
}

// hydrateUnboundRelationship reads a packstream structure and hydrate an UnboundRelationship with its data.
// If the structure does not represent a valid UnboundRelationship, it returns a types.ProtocolError.
func hydrateUnboundRelationship(rs *types.UnboundRelationship, st *packstream.Structure) error {
	var (
		ID     int64
		label  string
		convOK bool
		props  map[string]interface{}
	)

	if len(st.Fields) != 3 {
		return types.ErrProtocol
	}

	// ID
	if ID, convOK = st.Fields[0].(int64); !convOK {
		return types.ErrProtocol
	}
	rs.ID = uint64(ID)

	// Label
	if label, convOK = st.Fields[1].(string); !convOK {
		return types.ErrProtocol
	}
	rs.Type = label

	// Properties
	if props, convOK = st.Fields[2].(map[string]interface{}); !convOK {
		return types.ErrProtocol
	}
	rs.Properties = props
	return nil
}

// hydrateRelationship reads a packstream structure and hydrate an UnboundRelationship with its data.
// If the structure does not represent a valid Relationship, it returns a types.ProtocolError.
func hydrateRelationship(rs *types.Relationship, st *packstream.Structure) error {
	var (
		ID     int64
		label  string
		convOK bool
		fromID int64
		toID   int64
		props  map[string]interface{}
	)

	if len(st.Fields) != 5 {
		return types.ErrProtocol
	}

	// ID
	if ID, convOK = st.Fields[0].(int64); !convOK {
		return types.ErrProtocol
	}
	rs.ID = uint64(ID)

	// FromID
	if fromID, convOK = st.Fields[1].(int64); !convOK {
		return types.ErrProtocol
	}
	rs.FromID = uint64(fromID)

	// ToID
	if toID, convOK = st.Fields[2].(int64); !convOK {
		return types.ErrProtocol
	}
	rs.ToID = uint64(toID)

	// Type
	if label, convOK = st.Fields[3].(string); !convOK {
		return types.ErrProtocol
	}
	rs.Type = label

	// Properties
	if props, convOK = st.Fields[4].(map[string]interface{}); !convOK {
		return types.ErrProtocol
	}
	rs.Properties = props
	return nil
}

// hydratePath reads a packstream structure and hydrate a Path with its data.
// If the structure does not represent a valid Path, it returns a types.ProtocolError.
func hydratePath(p *types.Path, st *packstream.Structure) error {
	var (
		convOK   bool
		list     []interface{}
		convSt   packstream.Structure
		gen      interface{}
		node     *types.Node
		rs       *types.UnboundRelationship
		i        int64
		sequence []int64
		err      error
	)

	if len(st.Fields) != 3 {
		return types.ErrProtocol
	}

	// Nodes
	if list, convOK = st.Fields[0].([]interface{}); !convOK {
		return types.ErrProtocol
	}
	if len(list) == 0 {
		return nil
	}
	p.Nodes = make([]*types.Node, len(list))
	for i, item := range list {
		if convSt, convOK = item.(packstream.Structure); !convOK {
			return types.ErrProtocol
		}
		if gen, err = structRecordToType(&convSt); err != nil {
			return types.ErrProtocol
		}
		if node, convOK = gen.(*types.Node); !convOK {
			return types.ErrProtocol
		}
		p.Nodes[i] = node
	}

	// Relationships
	if list, convOK = st.Fields[1].([]interface{}); !convOK {
		return types.ErrProtocol
	}
	p.Relationships = make([]*types.Relationship, len(list))
	for i, item := range list {
		if convSt, convOK = item.(packstream.Structure); !convOK {
			return types.ErrProtocol
		}
		if gen, err = structRecordToType(&convSt); err != nil {
			return types.ErrProtocol
		}
		if rs, convOK = gen.(*types.UnboundRelationship); !convOK {
			return types.ErrProtocol
		}
		p.Relationships[i] = new(types.Relationship)
		p.Relationships[i].ID = rs.ID
		p.Relationships[i].Properties = rs.Properties
	}

	// Sequence
	if list, convOK = st.Fields[2].([]interface{}); !convOK {
		return types.ErrProtocol
	}
	if len(list)%2 != 0 {
		return types.ErrProtocol
	}
	for _, item := range list {
		if i, convOK = item.(int64); !convOK {
			return types.ErrProtocol
		}
		sequence = append(sequence, i)
	}

	// Binding
	lastNode := p.Nodes[0]
	length := len(sequence) / 2
	for i := 0; i < length; i++ {
		relIndex := sequence[i*2]
		if relIndex == 0 {
			return types.ErrProtocol
		}
		if sequence[2*i+1] >= int64(len(p.Nodes)) {
			return types.ErrProtocol
		}
		nextNode := p.Nodes[sequence[2*i+1]]
		if relIndex > 0 {
			if relIndex-1 > int64(len(p.Relationships)) {
				return types.ErrProtocol
			}
			p.Relationships[relIndex-1].FromID = lastNode.ID
			p.Relationships[relIndex-1].From = lastNode
			p.Relationships[relIndex-1].ToID = nextNode.ID
			p.Relationships[relIndex-1].End = nextNode
		} else {
			if -relIndex-1 > int64(len(p.Relationships)) {
				return types.ErrProtocol
			}
			p.Relationships[-relIndex-1].FromID = nextNode.ID
			p.Relationships[-relIndex-1].From = nextNode
			p.Relationships[-relIndex-1].ToID = lastNode.ID
			p.Relationships[-relIndex-1].End = lastNode
		}
		lastNode = nextNode
	}
	return nil
}

// recordToType tries to convert a value to a type from the types subpackage : If the value is a packstream Structure,
// it calls structRecordToType, if it is a slice or a map, it recursively calls recordToType for each value .
func recordToType(v interface{}) (interface{}, error) {
	switch v.(type) {
	default:
		return v, nil
	case packstream.Structure:
		st := v.(packstream.Structure)
		return structRecordToType(&st)
	case []interface{}:
		l := v.([]interface{})
		for i, item := range l {
			if vv, err := recordToType(item); err != nil {
				l[i] = vv
			} else {
				return nil, err
			}
		}
		return l, nil
	case map[string]interface{}:
		m := v.(map[string]interface{})
		for k, item := range m {
			if vv, err := recordToType(item); err != nil {
				m[k] = vv
			} else {
				return nil, err
			}
		}
		return m, nil
	}
}

// structRecordToType tries to convert a packstream Structure to a Node, Relationship, UnboundRelationship or a Path.
func structRecordToType(st *packstream.Structure) (_ interface{}, err error) {
	switch st.Signature {
	default:
		return nil, types.ErrProtocol
	case byte("N"[0]):
		res := new(types.Node)
		if err = hydrateNode(res, st); err != nil {
			return nil, err
		}
		return res, nil
	case byte("R"[0]):
		res := new(types.Relationship)
		if err = hydrateRelationship(res, st); err != nil {
			return nil, err
		}
		return res, nil
	case byte("r"[0]):
		res := new(types.UnboundRelationship)
		if err = hydrateUnboundRelationship(res, st); err != nil {
			return nil, err
		}
		return res, nil
	case byte("P"[0]):
		res := new(types.Path)
		if err = hydratePath(res, st); err != nil {
			return nil, err
		}
		return res, nil
	}
}
