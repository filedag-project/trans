package trans

import "sync"

var init_buf_size = 4<<20 + 256

var (
	headerBuf sync.Pool
	vBuf      sync.Pool
)

func init() {
	headerBuf.New = func() interface{} {
		return make([]byte, header_size)
	}
	vBuf.New = func() interface{} {
		return buffer(make([]byte, init_buf_size))
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
