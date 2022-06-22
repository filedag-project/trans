package trans

import "sync"

const VBUF_4M = 4 << 20
const VBUF_1M = 1 << 20

var v_buf_size = VBUF_1M + 256

const short_buf_size = 4 << 10

var (
	headerBuf sync.Pool
	vBuf      sync.Pool
	shortBuf  sync.Pool
)

func init() {
	headerBuf.New = func() interface{} {
		b := make([]byte, header_size)
		return &b
	}
	vBuf.New = func() interface{} {
		b := make([]byte, v_buf_size)
		return &b
	}
	shortBuf.New = func() interface{} {
		b := make([]byte, short_buf_size)
		return &b
	}
}

type buffer []byte

func (v *buffer) size(size int) {
	if cap(*v) < size {
		old := *v
		*v = make([]byte, size, size+short_buf_size)
		copy(*v, old)
	}
	*v = (*v)[:size]
}

func SetVBuf4M() {
	v_buf_size = VBUF_4M + 256
}
