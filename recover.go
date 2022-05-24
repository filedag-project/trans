package trans

import (
	"bufio"
	"fmt"
	"io"
	"math/rand"
	"os"
	"strings"
)

type RecoverClient struct {
	chunkClients   []*ClientWithInfo
	activeClients  []*ClientWithInfo
	recoverClients []*ClientWithInfo
	dataShards     int
	parShards      int
}

type ClientWithInfo struct {
	Client  Client
	Index   int
	Recover bool
}

func NewRecoverClient(chunkClients []*ClientWithInfo, dataShards, parShards int) (*RecoverClient, error) {
	if dataShards < 3 {
		return nil, fmt.Errorf("data shards should not be less than 3")
	}
	if parShards < 1 {
		return nil, fmt.Errorf("parity shards should not be less than 1")
	}
	if len(chunkClients) != dataShards+parShards {
		return nil, fmt.Errorf("number of chunk servers should be equal with sum of data shards and parity shards")
	}
	rc := &RecoverClient{
		chunkClients:   chunkClients,
		activeClients:  make([]*ClientWithInfo, 0),
		recoverClients: make([]*ClientWithInfo, 0),
		dataShards:     dataShards,
		parShards:      parShards,
	}
	for _, item := range chunkClients {
		if item.Recover {
			rc.recoverClients = append(rc.recoverClients, item)
		} else {
			rc.activeClients = append(rc.activeClients, item)
		}
	}
	if len(rc.activeClients) < dataShards {
		return nil, fmt.Errorf("exppect at least %d active clients, but only got %d ", dataShards, len(rc.activeClients))
	}

	return rc, nil
}

func (ec *RecoverClient) Recover(startKey string) (err error) {
	idx := rand.Intn(len(ec.activeClients))
	keyChan, err := ec.activeClients[idx].Client.AllKeysChan(startKey)
	if err != nil {
		return fmt.Errorf("RecoverClient failed to call AllKeysChan, idx: %d, err: %s", idx, err)
	}
	for key := range keyChan {
		ec.RecoverKey(key)
	}
	return
}

func (ec *RecoverClient) Close() {
	for _, client := range ec.chunkClients {
		client.Client.Close()
	}
}

func (ec *RecoverClient) ExportAllKeys(startKey, pathToAllKeys string) error {
	f, err := os.OpenFile(pathToAllKeys, os.O_CREATE|os.O_RDWR, 0755)
	if err != nil {
		return err
	}
	defer f.Close()

	idx := rand.Intn(len(ec.activeClients))
	keyChan, err := ec.activeClients[idx].Client.AllKeysChan(startKey)
	if err != nil {
		return fmt.Errorf("RecoverClient failed to call AllKeysChan, idx: %d, err: %s", idx, err)
	}
	for key := range keyChan {
		if _, err = f.WriteString(fmt.Sprintf("%s\n", key)); err != nil {
			return err
		}
	}
	return nil
}

func (ec *RecoverClient) RecoverFromFile(startKey, pathToAllKeys string) (err error) {
	foundStartKey := false
	f, err := os.Open(pathToAllKeys)
	if err != nil {
		return err
	}
	defer f.Close()
	liner := bufio.NewReader(f)
	for {
		line, err := liner.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				fmt.Println("end with input file")
			} else {
				logger.Error(err)
			}
			break
		}
		key := strings.TrimSpace(line)

		if startKey != "" && !foundStartKey {
			if startKey != key {
				continue
			}
			foundStartKey = true
		}
		ec.RecoverKey(key)
	}
	return
}

func (ec *RecoverClient) RecoverKey(key string) (err error) {
	// first to make sure that the key doesn't exist in at least one of the recover clients
	var needRecover bool
	for _, recoverClient := range ec.recoverClients {
		if has, _ := recoverClient.Client.Has(key); !has {
			needRecover = true
			break
		}
	}
	if !needRecover { // recover clients already has the key, just go to net key to recover
		return nil
	}
	ch := make(chan ecres)
	shardsNum := ec.dataShards + ec.parShards
	for _, client := range ec.activeClients {
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
		}(client.Index, client.Client, ch)
	}

	shards := make([][]byte, shardsNum)
	actNum := len(ec.activeClients)
	for i := 0; i < actNum; i++ {
		res := <-ch
		if res.e != nil {
			logger.Warnf("fetch index: %d shard failed: %s", res.i, res.e)
		} else {
			shards[res.i] = res.v
		}
	}

	recShards, err := ErasueRecover(shards, ec.dataShards, ec.parShards)
	if err != nil {
		return err
	}
	ch2 := make(chan string)
	for _, recoverClient := range ec.recoverClients {
		go func(recoverClient *ClientWithInfo, ch2 chan string) {
			if has, _ := recoverClient.Client.Has(key); !has {
				if err := recoverClient.Client.Put(key, recShards[recoverClient.Index]); err != nil {
					logger.Error(err)
					ch2 <- "failed"
				} else {
					ch2 <- "done"
				}
			} else {
				ch2 <- "skip"
			}
		}(recoverClient, ch2)
	}
	for range ec.recoverClients {
		logger.Info(<-ch2)
	}
	return
}
