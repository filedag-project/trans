package trans

import (
	"testing"

	"context"

	kv "github.com/filedag-project/mutcask"
)

func TestPServ(t *testing.T) {
	db := kv.NewMemkv()
	serv, err := NewPServ(context.TODO(), ":3512", db, "tcp")
	if err != nil {
		t.Fatal("failed to instance PServ ", err)
	}
	defer serv.Close()
}
