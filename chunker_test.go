package trans

import (
	"bytes"
	"io"
	"testing"
)

func TestChunker(t *testing.T) {
	sentence := []byte("bits of time can have bits of joy")
	size := 2
	r1 := bytes.NewReader(sentence)
	ch01 := NewChunker(r1, size)
	for i := 0; i*size < len(sentence); i++ {
		bs, err := ch01.NextBytes()
		if err != nil && err != io.EOF {
			t.Fatal(err)
		}
		start := i * size
		end := (i + 1) * size
		if end > len(sentence) {
			end = len(sentence)
		}
		if !bytes.Equal(bs, sentence[start:end]) {
			t.Fatal(bs, sentence[start:end])
		}
	}
}
