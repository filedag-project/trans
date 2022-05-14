package trans

import "io"

func NewChunker(r io.Reader, size int) *Chunker {
	return &Chunker{r, size, nil}

}

type Chunker struct {
	r    io.Reader
	size int
	err  error
}

func (k *Chunker) NextBytes() ([]byte, error) {
	if k.err != nil {
		return nil, k.err
	}
	full := make([]byte, k.size)
	n, err := io.ReadFull(k.r, full)
	switch err {
	case io.ErrUnexpectedEOF:
		k.err = io.EOF
		small := make([]byte, n)
		copy(small, full)
		return small, nil
	case nil:
		return full, nil
	default:
		return nil, err
	}
}
