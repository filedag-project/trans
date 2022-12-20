package trans

import "sync"

const VBUF_4M = 4 << 20
const VBUF_1M = 1 << 20

var v_buf_size = VBUF_1M + 256

var (
	vBuf sync.Pool
)

func init() {
	vBuf.New = func() interface{} {
		b := make([]byte, v_buf_size)
		return &b
	}
}

type buffer []byte

func (v *buffer) size(size int) {
	if cap(*v) < size {
		old := *v
		*v = make([]byte, size)
		copy(*v, old)
	}
	*v = (*v)[:size]
}

func SetVBuf4M() {
	v_buf_size = VBUF_4M + 256
}
