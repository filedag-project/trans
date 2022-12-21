package trans

import (
	"fmt"
	"strconv"

	kv "github.com/filedag-project/mutcask"
	"github.com/fxamacker/cbor/v2"
)

func i2b(n int) []byte {
	return []byte(fmt.Sprintf("%d", n))
}

func b2i(b []byte) (int, error) {
	n, err := strconv.ParseInt(string(b), 10, 32)
	if err != nil {
		return 0, err
	}

	return int(n), nil
}

func DecodeKVPair(buf []byte) (ps []kv.Pair, err error) {
	err = cbor.Unmarshal(buf, &ps)
	return
}

func EncodeKVPair(ps []kv.Pair) ([]byte, error) {
	return cbor.Marshal(ps)
}

func DecodeKeys(buf []byte) (keys [][]byte, err error) {
	err = cbor.Unmarshal(buf, &keys)
	return
}

func EncodeKeys(keys [][]byte) ([]byte, error) {
	return cbor.Marshal(keys)
}
