package resp

import (
	"strconv"
	"strings"
)

// Kind identifies the RESP2 type of a Value. The constants are the type-prefix
// bytes of the wire format.
type Kind byte

// The five RESP2 data types.
const (
	SimpleString Kind = '+'
	Error        Kind = '-'
	Integer      Kind = ':'
	Bulk         Kind = '$'
	Array        Kind = '*'
)

// Value is a decoded RESP2 reply. Which fields are meaningful depends on Kind:
//
//	SimpleString, Error, Bulk -> Str
//	Integer                   -> Int
//	Array                     -> Array
//
// Null is set for the null bulk string ($-1) and the null array (*-1).
//
// Requests sent by clients are not Values: they are plain [][]byte, see
// Reader.ReadCommand.
type Value struct {
	Kind  Kind
	Str   []byte
	Int   int64
	Array []Value
	Null  bool
}

// String renders a Value in a compact, human readable form (useful in test
// failures and in debugging output).
func (v Value) String() string {
	switch v.Kind {
	case SimpleString:
		return "+" + string(v.Str)
	case Error:
		return "-" + string(v.Str)
	case Integer:
		return ":" + strconv.FormatInt(v.Int, 10)
	case Bulk:
		if v.Null {
			return "(nil)"
		}
		return strconv.Quote(string(v.Str))
	case Array:
		if v.Null {
			return "(nil array)"
		}
		parts := make([]string, len(v.Array))
		for i, e := range v.Array {
			parts[i] = e.String()
		}
		return "[" + strings.Join(parts, ", ") + "]"
	}
	return "(invalid value)"
}
