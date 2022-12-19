package trans

// import (
// 	"bytes"
// 	"context"
// 	"testing"
// 	"time"

// 	kv "github.com/filedag-project/mutcask"
// 	logging "github.com/ipfs/go-log/v2"
// )

// func TestQuicClient(t *testing.T) {
// 	logging.SetLogLevel("*", "info")
// 	ctx := context.Background()
// 	addr := ":3420"
// 	db := kv.NewMemkv()
// 	serv, err := NewQuicServ(ctx, addr, db)
// 	if err != nil {
// 		t.Fatal("failed to instance PServ ", err)
// 	}
// 	// wait for server setup listener
// 	time.Sleep(time.Millisecond * 100)
// 	defer serv.Close()
// 	client := NewQuicClient(ctx, "127.0.0.1"+addr, 10)
// 	defer client.Close()

// 	// test put data
// 	for _, d := range tdata {
// 		if err := client.Put(d.k, d.v); err != nil {
// 			t.Fatal("put data failed ", err)
// 		}
// 	}

// 	// test get data
// 	for _, d := range tdata {
// 		v, err := client.Get(d.k)
// 		if err != nil {
// 			t.Fatal(err)
// 		}
// 		if !bytes.Equal(v, d.v) {
// 			t.Logf("v: %#v", v)
// 			t.Logf("d.v: %s", d.v)
// 			t.Fatal("value not match")
// 		}
// 	}
// 	// test get size of data
// 	for _, d := range tdata {
// 		size, err := client.Size(d.k)
// 		if err != nil {
// 			t.Fatal(err)
// 		}
// 		if len(d.v) != size {
// 			t.Fatal("size not match")
// 		}
// 	}
// 	// test has data
// 	for _, d := range tdata {
// 		has, err := client.Has(d.k)
// 		if err != nil {
// 			t.Fatal(err)
// 		}
// 		if !has {
// 			t.Fatal("should has key ", d.k)
// 		}
// 	}
// 	kc, err := client.AllKeysChan("")
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	for k := range kc {
// 		t.Log(k)
// 	}
// 	// test delete data
// 	for _, d := range tdata {
// 		err := client.Delete(d.k)
// 		if err != nil {
// 			t.Fatal(err)
// 		}
// 	}

// 	// test error not found
// 	for _, d := range tdata {
// 		_, err := client.Size(d.k)
// 		if err != ErrNotFound {
// 			t.Fatal(err)
// 		}
// 	}
// }
