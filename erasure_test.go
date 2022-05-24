package trans

import (
	"bytes"
	"context"
	"testing"
	"time"

	kv "github.com/filedag-project/mutcask"
	logging "github.com/ipfs/go-log/v2"
)

func TestErasureClient(t *testing.T) {
	ctx := context.Background()
	addrList := []string{":3312", ":3313", ":3314", ":3315", ":3316"}
	chunkServers := make([]*PServ, len(addrList))
	for i, addr := range addrList {
		db := kv.NewMemkv()
		serv, err := NewPServ(ctx, addr, db)
		if err != nil {
			t.Fatal("failed to instance PServ ", err)
		}
		chunkServers[i] = serv
		defer serv.Close()
	}
	// wait for server setup listener
	time.Sleep(time.Millisecond * 200)

	chunkClients := make([]Client, len(addrList))
	for i, addr := range addrList {
		cli := NewTransClient(ctx, "127.0.0.1"+addr, 1)
		defer cli.Close()
		chunkClients[i] = cli
	}

	client, err := NewErasureClient(chunkClients, 3, 2, "")
	if err != nil {
		t.Fatal(err)
	}

	// test put data
	for _, d := range tdata {
		if err := client.Put(d.k, d.v); err != nil {
			t.Fatal("put data failed ", err)
		}
	}

	// test get data
	for _, d := range tdata {
		v, err := client.Get(d.k)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(v[:len(d.v)], d.v) {
			t.Fatal("value not match")
		}
	}

	// test has data
	for _, d := range tdata {
		has, err := client.Has(d.k)
		if err != nil {
			t.Fatal(err)
		}
		if !has {
			t.Fatal("should has key ", d.k)
		}
	}

	// test all keys chan
	kc, err := client.AllKeysChan("")
	if err != nil {
		t.Fatal(err)
	}
	for k := range kc {
		t.Log(k)
	}

	// test delete data
	for _, d := range tdata {
		err := client.Delete(d.k)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestErasureClientRecoverMode(t *testing.T) {
	ctx := context.Background()
	addrList := []string{":3212", ":3213", ":3214", ":3215", ":3216"}
	chunkServers := make([]*PServ, len(addrList))
	for i, addr := range addrList {
		db := kv.NewMemkv()
		serv, err := NewPServ(ctx, addr, db)
		if err != nil {
			t.Fatal("failed to instance PServ ", err)
		}
		chunkServers[i] = serv
		defer serv.Close()
	}
	// wait for server setup listener
	time.Sleep(time.Millisecond * 200)

	chunkClients := make([]Client, len(addrList))
	for i, addr := range addrList {
		cli := NewTransClient(ctx, "127.0.0.1"+addr, 1)
		defer cli.Close()
		chunkClients[i] = cli
	}

	client, err := NewErasureClient(chunkClients, 3, 2, RecoverMode)
	if err != nil {
		t.Fatal(err)
	}

	// test put data
	for _, d := range tdata {
		if err := client.Put(d.k, d.v); err != nil {
			t.Fatal("put data failed ", err)
		}
	}

	recoverIdx := 2
	for _, d := range tdata {
		err := chunkClients[recoverIdx].Delete(d.k)
		if err != nil {
			t.Fatal(err)
		}
	}

	// get data with recover mode
	for _, d := range tdata {
		v, err := client.Get(d.k)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(v[:len(d.v)], d.v) {
			t.Fatal("value not match")
		}
	}
	// data should been restored
	for _, d := range tdata {
		_, err := chunkClients[recoverIdx].Get(d.k)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestDecode(t *testing.T) {
	logging.SetLogLevel("*", "info")
	data := []byte("Reed-Solomon Erasure Coding in Go, with speeds exceeding 1GB/s/cpu core implemented in pure Go.")
	dataShards := 4
	parShards := 1
	shards, err := ErasueEncode(data, dataShards, parShards)
	if err != nil {
		t.Fatal(err)
	}
	if len(shards) != dataShards+parShards {
		t.Fatal("shards number not match")
	}

	d, _, err := ErasueDecode(shards, dataShards, parShards)
	if err != nil {
		t.Fatal(err)
	}
	if len(d) < len(data) {
		t.Fatal("decoded data too short")
	}
	if !bytes.Equal(data, d[:len(data)]) {
		t.Fatal("decoded data not match")
	}
}

func TestEncode(t *testing.T) {
	logging.SetLogLevel("*", "info")
	data := []byte("Reed-Solomon Erasure Coding in Go, with speeds exceeding 1GB/s/cpu core implemented in pure Go.")
	dataShards := 4
	parShards := 1
	shards, err := ErasueEncode(data, dataShards, parShards)
	if err != nil {
		t.Fatal(err)
	}
	if len(shards) != dataShards+parShards {
		t.Fatal("shards number not match")
	}
}
