package trans

import (
	"bytes"
	"fmt"
	"math/rand"
	"time"

	"github.com/klauspost/reedsolomon"
)

var _ Client = (*ErasureClient)(nil)

type ErasureClient struct {
	chunkClients []Client
	dataShards   int
	parShards    int
}

func NewErasureClient(chunkClients []Client, dataShards, parShards int) (*ErasureClient, error) {
	if dataShards < 3 {
		return nil, fmt.Errorf("data shards should not be less than 3")
	}
	if parShards < 1 {
		return nil, fmt.Errorf("parity shards should not be less than 1")
	}
	if len(chunkClients) != dataShards+parShards {
		return nil, fmt.Errorf("number of chunk servers should be equal with sum of data shards and parity shards")
	}
	return &ErasureClient{
		chunkClients: chunkClients,
		dataShards:   dataShards,
		parShards:    parShards,
	}, nil
}

func (ec *ErasureClient) Size(key string) (n int, err error) {
	activeClients := ec.activeClients()
	idx := rand.Intn(len(activeClients))
	s, err := activeClients[idx].Size(key)
	if err != nil {
		return
	}
	return s * len(ec.chunkClients), nil
}

func (ec *ErasureClient) Has(key string) (has bool, err error) {
	activeClients := ec.activeClients()
	idx := rand.Intn(len(activeClients))

	return activeClients[idx].Has(key)
}

func (ec *ErasureClient) Delete(key string) (err error) {
	// not support right now
	return nil
}

func (ec *ErasureClient) Get(key string) (value []byte, err error) {
	shardsNum := len(ec.chunkClients)
	ch := make(chan ecres)
	for i, client := range ec.chunkClients {
		go func(idx int, client Client, ch chan ecres) {
			res := ecres{
				i: idx,
			}
			v, err := client.Get(key)
			if err != nil {
				res.e = err
			} else {
				res.v = v
			}
			ch <- res
		}(i, client, ch)
	}
	shards := make([][]byte, shardsNum)
	for i := 0; i < shardsNum; i++ {
		res := <-ch
		if res.e != nil {
			logger.Warnf("fetch index: %d shard failed: %s", res.i, res.e)
		} else {
			shards[res.i] = res.v
		}
	}
	value, err = erasue_decode(shards, ec.dataShards, ec.parShards)
	return
}

func (ec *ErasureClient) Put(key string, value []byte) (err error) {
	shards, err := ErasueEncode(value, ec.dataShards, ec.parShards)
	if err != nil {
		return err
	}
	shardsNum := len(shards)
	ch := make(chan ecres)
	for i, client := range ec.chunkClients {
		go func(idx int, client Client, ch chan ecres, v []byte) {
			res := ecres{
				i: idx,
			}
			err := client.Put(key, v)
			if err != nil {
				res.e = err
			}
			ch <- res
		}(i, client, ch, shards[i])
	}
	for i := 0; i < shardsNum; i++ {
		res := <-ch
		if res.e != nil {
			logger.Errorf("put index: %d shard failed: %s", res.i, res.e)
			err = res.e
		}
	}
	return
}

func (ec *ErasureClient) AllKeysChan(startKey string) (chan string, error) {
	activeClients := ec.activeClients()
	idx := rand.Intn(len(activeClients))
	return activeClients[idx].AllKeysChan(startKey)
}

func (ec *ErasureClient) Close() {
	for _, client := range ec.chunkClients {
		client.Close()
	}
}

func (ec *ErasureClient) activeClients() []Client {
	res := make([]Client, 0)
	for _, client := range ec.chunkClients {
		if client.TargetActive() {
			res = append(res, client)
		}
	}
	return res
}

func (ec *ErasureClient) TargetActive() bool {
	for _, client := range ec.chunkClients {
		if !client.TargetActive() {
			return false
		}
	}
	return true
}

func ErasueEncode(data []byte, dataShards int, parShards int) (shards [][]byte, err error) {
	enc, err := reedsolomon.New(dataShards, parShards)
	if err != nil {
		return
	}
	t := time.Now()
	shards, err = enc.Split(data)
	if err != nil {
		return
	}
	logger.Warnf("erasure split dur: %d", time.Since(t).Microseconds())
	t = time.Now()
	err = enc.Encode(shards)
	if err != nil {
		return
	}
	logger.Warnf("erasure encode dur: %d", time.Since(t).Microseconds())
	return
}

func ErasueDecode(shards [][]byte, dataShards int, parShards int) (data []byte, err error) {
	enc, err := reedsolomon.New(dataShards, parShards)
	if err != nil {
		return
	}
	r := bytes.NewBuffer([]byte{})
	t := time.Now()
	ok, _ := enc.Verify(shards)
	logger.Warnf("erasure verify dur: %d", time.Since(t).Microseconds())
	if !ok {
		logger.Warn("erasure verify failed, going to reconstruct")
		t = time.Now()
		err = enc.Reconstruct(shards)
		logger.Warnf("erasure reconstruction dur: %d", time.Since(t).Microseconds())
		if err != nil {
			logger.Error("erasure reconstruction failed")
			return
		}
	}

	if err = enc.Join(r, shards, len(shards[0])*dataShards); err != nil {
		return
	}
	data = r.Bytes()
	return
}

type ecres struct {
	e error
	i int
	v []byte
}

func erasue_decode(shards [][]byte, dataShards int, parShards int) (data []byte, err error) {
	bs := bytes.NewBuffer([]byte{})
	var needReconstructed bool
	var shardLen int
	for i := 0; i < dataShards; i++ {
		if shards[i] == nil {
			needReconstructed = true
			break
		}
		n, err := bs.Write(shards[i])
		if err != nil {
			return nil, err
		}
		if shardLen > 0 && shardLen != n {
			return nil, fmt.Errorf("shards should has equal size, %d / %d", shardLen, n)
		}
		shardLen = n
	}
	if !needReconstructed {
		return bs.Bytes(), nil
	}
	return ErasueDecode(shards, dataShards, parShards)
}
