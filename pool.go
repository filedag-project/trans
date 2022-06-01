package trans

import "sync"

const v_buf_size = 4<<20 + 256
const short_buf_size = 4 << 10

var (
	headerBuf sync.Pool
	vBuf      sync.Pool
	shortBuf  sync.Pool
)

func init() {
	headerBuf.New = func() interface{} {
		return make([]byte, header_size)
	}
	vBuf.New = func() interface{} {
		return buffer(make([]byte, v_buf_size))
	}
	shortBuf.New = func() interface{} {
		return buffer(make([]byte, short_buf_size))
	}
}

type buffer []byte

func (v *buffer) size(size int) {
	if cap(*v) < size {
		old := *v
		*v = make([]byte, size, 2*size)
		copy(*v, old)
	}
	*v = (*v)[:size]
}
