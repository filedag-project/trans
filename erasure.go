package trans

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	kv "github.com/filedag-project/mutcask"
	"github.com/klauspost/reedsolomon"
)

const RecoverMode = "recover"

var _ Client = (*ErasureClient)(nil)

type ErasureClient struct {
	chunkClients []Client
	dataClients  []Client
	parClients   []Client
	dataShards   int
	parShards    int
	mode         string
}

func NewErasureClient(chunkClients []Client, dataShards, parShards int, mode string) (*ErasureClient, error) {
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
		dataClients:  chunkClients[:dataShards],
		parClients:   chunkClients[dataShards:],
		dataShards:   dataShards,
		parShards:    parShards,
		mode:         mode,
	}, nil
}

func (ec *ErasureClient) Size(key []byte) (n int, err error) {
	shardsNum := len(ec.chunkClients)
	ch := make(chan ecres)
	for i, client := range ec.chunkClients {
		go func(idx int, client Client, ch chan ecres) {
			res := ecres{
				i: idx,
			}
			s, err := client.Size(key)
			if err != nil {
				res.e = err
			} else {
				res.s = s
			}
			ch <- res
		}(i, client, ch)
	}
	failedCount := 0
	shardSize := 0
	for i := 0; i < shardsNum; i++ {
		res := <-ch
		if res.e != nil {
			logger.Warnf("fetch index: %d shard failed: %s", res.i, res.e)
			failedCount++
		} else {
			shardSize = res.s
		}
	}
	if failedCount > ec.parShards { // if there is not enougth shards to reconstruct data, just respond as not found
		return -1, ErrNotFound
	}

	return shardSize * len(ec.chunkClients), nil
}

func (ec *ErasureClient) Has(key []byte) (has bool, err error) {
	_, err = ec.Size(key)
	if err != nil {
		if err == ErrNotFound {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (ec *ErasureClient) Delete(key []byte) (err error) {
	// not support right now
	return nil
}

func (ec *ErasureClient) CheckSum(key []byte) (uint32, error) {
	return 0, kv.ErrNotImpl
}

func (ec *ErasureClient) Get(key []byte) (value []byte, err error) {
	if ec.mode == RecoverMode {
		return ec.recoverAfterGet(key)
	}
	return ec.get(key)
}

func (ec *ErasureClient) get(key []byte) (value []byte, err error) {
	shardsNum := len(ec.chunkClients)
	dataShardsNum := len(ec.dataClients)
	parShardsNum := len(ec.parClients)
	ch := make(chan ecres)
	nilDataCount := 0
	shards := make([][]byte, shardsNum)
	// at first only get data chunks from data clients
	for i, client := range ec.dataClients {
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
	for i := 0; i < dataShardsNum; i++ {
		res := <-ch
		if res.e != nil {
			logger.Warnf("fetch index: %d shard failed: %s", res.i, res.e)
			nilDataCount++
		} else {
			shards[res.i] = res.v
		}
	}
	if nilDataCount == 0 { // success fetch all chunks from data clients
		value, _, err = erasue_decode(shards, ec.dataShards, ec.parShards, ec.mode)
		if err != nil {
			return nil, err
		}
		value, err = unwrapValue(value)
		return
	}
	if nilDataCount > ec.parShards { // if there is not enougth shards to reconstruct data, just respond as not found
		return nil, ErrNotFound
	}

	// then get par chunks from par clients if there are failed fetchs from data clients
	// neet to get par chunks to construct data back
	for i, client := range ec.parClients {
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
		}(i+dataShardsNum, client, ch)
	}
	for i := 0; i < parShardsNum; i++ {
		res := <-ch
		if res.e != nil {
			logger.Warnf("fetch index: %d shard failed: %s", res.i, res.e)
			nilDataCount++
		} else {
			shards[res.i] = res.v
		}
	}
	if nilDataCount > ec.parShards { // if there is not enougth shards to reconstruct data, just respond as not found
		return nil, ErrNotFound
	}
	value, _, err = erasue_decode(shards, ec.dataShards, ec.parShards, ec.mode)
	if err != nil {
		return nil, err
	}
	value, err = unwrapValue(value)
	if err != nil {
		return
	}
	return
}

func (ec *ErasureClient) recoverAfterGet(key []byte) (value []byte, err error) {
	shardsNum := len(ec.chunkClients)
	ch := make(chan ecres)
	nilDataCount := 0
	recoverIndex := make([]int, 0)
	shards := make([][]byte, shardsNum)

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
	for i := 0; i < shardsNum; i++ {
		res := <-ch
		if res.e != nil {
			logger.Warnf("fetch index: %d shard failed: %s", res.i, res.e)
			nilDataCount++
			recoverIndex = append(recoverIndex, res.i)
		} else {
			shards[res.i] = res.v
		}
	}
	if nilDataCount == 0 { // success fetch all chunks from data clients
		value, _, err = erasue_decode(shards, ec.dataShards, ec.parShards, "")
		if err != nil {
			return nil, err
		}
		value, err = unwrapValue(value)
		return
	}
	if nilDataCount > ec.parShards { // if there is not enougth shards to reconstruct data, just respond as not found
		return nil, ErrNotFound
	}

	value, recoverShards, err := erasue_decode(shards, ec.dataShards, ec.parShards, ec.mode)
	if err != nil {
		return nil, err
	}
	value, err = unwrapValue(value)
	if err != nil {
		return
	}
	// check if we need do data recover for this get action
	if nilDataCount > 0 && ec.mode == RecoverMode {
		if recoverShards == nil || len(recoverShards) != len(ec.chunkClients) {
			logger.Warnf("recover mode - need recover but lack of shards data")
			return
		}
		var wg sync.WaitGroup
		for _, idx := range recoverIndex {
			wg.Add(1)
			go func(idx int, client Client, key, value []byte) {
				defer func() {
					wg.Done()
				}()
				if err := client.Put(key, value); err != nil {
					logger.Errorf("recover mode - recover failed after get action for client:%d, key: %s, error: %s", idx, key, err)
				}
			}(idx, ec.chunkClients[idx], key, recoverShards[idx])
		}
		wg.Wait()
	}
	return
}

func (ec *ErasureClient) Put(key, value []byte) (err error) {
	shards, err := ErasueEncode(wrapValue(value), ec.dataShards, ec.parShards)
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
	var errCount int
	for i := 0; i < shardsNum; i++ {
		res := <-ch
		if res.e != nil {
			logger.Errorf("put index: %d shard failed: %s", res.i, res.e)
			err = res.e
			errCount++
		}
	}
	// make sure put action work even if some chunk server is down according to parity shards number
	if errCount <= ec.parShards {
		return nil
	}
	return
}

func (ec *ErasureClient) AllKeysChan(context.Context) (chan string, error) {
	return nil, kv.ErrNotImpl
}

func (ec *ErasureClient) Scan([]byte, int) ([]kv.Pair, error) {
	return nil, kv.ErrNotImpl
}

func (ec *ErasureClient) ScanKeys([]byte, int) ([][]byte, error) {
	return nil, kv.ErrNotImpl
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

func ErasueDecode(shards [][]byte, dataShards int, parShards int) (data []byte, recoverShards [][]byte, err error) {
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
	recoverShards = shards
	return
}

type ecres struct {
	e error
	i int
	v []byte
	s int
}

func erasue_decode(shards [][]byte, dataShards int, parShards int, mode string) (data []byte, recoverShards [][]byte, err error) {
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
			return nil, nil, err
		}
		if shardLen > 0 && shardLen != n {
			return nil, nil, fmt.Errorf("shards should has equal size, %d / %d", shardLen, n)
		}
		shardLen = n
	}
	if mode != RecoverMode && !needReconstructed {
		return bs.Bytes(), nil, nil
	}
	return ErasueDecode(shards, dataShards, parShards)
}

func wrapValue(v []byte) []byte {
	l := len(v)
	wv := make([]byte, l+4)
	copy(wv[4:], v)
	binary.LittleEndian.PutUint32(wv[:4], uint32(l))
	return wv
}

func unwrapValue(v []byte) ([]byte, error) {
	if len(v) < 4 {
		return nil, fmt.Errorf("erasure unwrapValue: value too short")
	}
	l := binary.LittleEndian.Uint32(v[:4])
	if len(v) < int(4+l) {
		return nil, fmt.Errorf("erasure unwrapValue: value size: %d, expected size: %d", len(v), 4+l)
	}
	return v[4 : 4+l], nil
}

func ErasueRecover(shards [][]byte, dataShards int, parShards int) (data [][]byte, err error) {
	enc, err := reedsolomon.New(dataShards, parShards)
	if err != nil {
		return
	}

	t := time.Now()
	err = enc.Reconstruct(shards)
	logger.Warnf("erasure reconstruction dur: %d", time.Since(t).Microseconds())
	if err != nil {
		logger.Error("erasure reconstruction failed")
		return
	}
	t = time.Now()
	if ok, _ := enc.Verify(shards); !ok {
		return nil, fmt.Errorf("erasure verify failed, going to reconstruct")
	}
	logger.Warnf("erasure verify dur: %d", time.Since(t).Microseconds())

	return shards, nil
}
