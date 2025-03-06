package deadsimpledb

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_serializeDeserializeValues(t *testing.T) {
	testCases := []struct {
		name string
		r    []value
	}{
		{
			name: "Non-null values",
			r: []value{
				newBlob([]byte("hello")),
				newInt64(123),
			},
		},
		{
			name: "Null values",
			r: []value{
				newNullValue(typeBlob),
				newNullValue(typeInt64),
			},
		},
		{
			name: "Mixed values",
			r: []value{
				newBlob([]byte("hello")),
				newNullValue(typeInt64),
				newInt64(123),
				newNullValue(typeBlob),
			},
		},
		{
			name: "int64",
			r: []value{
				newInt64(-123),
				newInt64(-1),
				newInt64(0),
			},
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("testcase_%d_%s", i+1, tc.name), func(t *testing.T) {
			serialized := new(bytes.Buffer)
			err := serializeValues(serialized, tc.r)
			require.NoError(t, err, "failed to serialze values")
			deserialized := make([]value, len(tc.r))
			for i, v := range tc.r {
				deserialized[i] = value{Type: v.Type}
			}
			err = deserializeValues(serialized, deserialized)
			require.NoError(t, err, "failed to deserialze values")
			require.Equal(t, tc.r, deserialized, "values not match")
		})
	}

}

var nullEscapeTestCases = []struct {
	unescape []byte
	escaped  []byte
}{
	{
		// no 0x00 or 0x01
		unescape: []byte{0x02, 0x03, 0x04},
		escaped:  []byte{0x02, 0x03, 0x04},
	},
	{
		unescape: []byte{0x00},
		escaped:  []byte{0x01, 0x01},
	},
	{
		unescape: []byte{0x01},
		escaped:  []byte{0x01, 0x02},
	},
	{
		unescape: []byte{0x00, 0x01, 0x02, 0x01},
		escaped:  []byte{0x01, 0x01, 0x01, 0x02, 0x02, 0x01, 0x02},
	},
}

func Test_escapeNull(t *testing.T) {
	for i, tc := range nullEscapeTestCases {
		t.Run(fmt.Sprintf("testcase_%d", i+1), func(t *testing.T) {
			out := escapeNull(tc.unescape)
			require.Equal(t, tc.escaped, out, "escaped bytes not match")
		})
	}
}

func Test_unescapeNull(t *testing.T) {
	for i, tc := range nullEscapeTestCases {
		t.Run(fmt.Sprintf("testcase_%d", i+1), func(t *testing.T) {
			out := unescapeNull(tc.escaped)
			require.Equal(t, tc.unescape, out, "unescaped bytes not match")
		})
	}
}

func Test_readNullTerminatedBlob(t *testing.T) {
	testCases := []struct {
		data []byte
		blob []byte
		fail bool
		err  error
	}{
		{
			data: []byte{0},
			blob: nil,
			fail: false,
		},
		{
			data: []byte{1, 2, 3, 0},
			blob: []byte{1, 2, 3},
			fail: false,
		},
		{
			data: []byte{1, 2, 3},
			blob: nil,
			fail: true,
			err:  io.EOF,
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("testcase_%d", i+1), func(t *testing.T) {
			blob, err := readNullTerminatedBlob(bytes.NewReader(tc.data))
			if tc.fail {
				require.NotNil(t, err, "expected error")
				if tc.err != nil {
					require.ErrorIs(t, err, tc.err, "error not match")
				}
			} else {
				require.NoError(t, err, "unexpected error")
				require.Equal(t, tc.blob, blob, "blob not match")
			}
		})
	}
}
