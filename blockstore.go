package trans

import (
	"context"
	"fmt"
	"strings"
	"sync"

	kv "github.com/filedag-project/mutcask"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	ipld "github.com/ipfs/go-ipld-format"
	"golang.org/x/xerrors"
)

const default_batch_num = 32

type blostore struct {
	kv    Client
	batch int
}

var _ blockstore.Blockstore = (*blostore)(nil)

func (bs *blostore) DeleteBlock(ctx context.Context, cid cid.Cid) error {
	return bs.kv.Delete(cid.String())
}

func (bs *blostore) Has(ctx context.Context, cid cid.Cid) (bool, error) {
	_, err := bs.kv.Size(cid.String())
	if err != nil {
		if err == kv.ErrNotFound {
			return false, nil
		}
		return false, err
	}

	return true, nil
}

func (bs *blostore) Get(ctx context.Context, cid cid.Cid) (blocks.Block, error) {
	data, err := bs.kv.Get(cid.String())
	if err != nil {
		if err == kv.ErrNotFound {
			return nil, ipld.ErrNotFound{Cid: cid}
		}
		return nil, err
	}

	b, err := blocks.NewBlockWithCid(data, cid)
	if err == blocks.ErrWrongHash {
		return nil, blockstore.ErrHashMismatch
	}
	return b, err
}

func (bs *blostore) GetSize(ctx context.Context, cid cid.Cid) (int, error) {
	n, err := bs.kv.Size(cid.String())
	if err != nil && err == kv.ErrNotFound {
		return -1, ipld.ErrNotFound{Cid: cid}
	}
	return n, err
}

func (bs *blostore) Put(ctx context.Context, blo blocks.Block) error {
	return bs.kv.Put(blo.Cid().String(), blo.RawData())
}

func (bs *blostore) PutMany(ctx context.Context, blos []blocks.Block) error {
	var errlist []string
	var wg sync.WaitGroup
	batchChan := make(chan struct{}, bs.batch)
	wg.Add(len(blos))
	for _, blo := range blos {
		go func(bs *blostore, blo blocks.Block) {
			defer func() {
				<-batchChan
			}()
			batchChan <- struct{}{}
			err := bs.kv.Put(blo.Cid().String(), blo.RawData())
			if err != nil {
				errlist = append(errlist, err.Error())
			}
		}(bs, blo)
	}
	wg.Wait()
	if len(errlist) > 0 {
		return xerrors.New(strings.Join(errlist, "\n"))
	}
	return nil
}

func (bs *blostore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	kchan, err := bs.kv.AllKeysChan("")
	if err != nil {
		return nil, err
	}
	ch := make(chan cid.Cid)
	go func(ch chan cid.Cid, kchan chan string) {
		defer close(ch)
		for cidstr := range kchan {
			id, err := cid.Decode(cidstr)
			if err != nil {
				fmt.Printf("AllKeysChan Error: %s\n", err)
				return
			}
			ch <- id
		}
	}(ch, kchan)
	return ch, nil
}

func (bs *blostore) HashOnRead(enabled bool) {
	// do nothing, as every read will check hash match or not
}

func NewEraBS(esclient Client, batch int) (*blostore, error) {
	if batch == 0 {
		batch = default_batch_num
	}

	return &blostore{
		kv:    esclient,
		batch: batch,
	}, nil
}

func NewErasureBlockstore(ctx context.Context, servAddrs []string, connNum int, dataShards, parShards int, batch int) (*blostore, error) {
	if batch == 0 {
		batch = default_batch_num
	}
	chunkClients := make([]Client, len(servAddrs))
	for i, addr := range servAddrs {
		chunkClients[i] = NewTransClient(ctx, addr, connNum)
	}
	era, err := NewErasureClient(chunkClients, dataShards, parShards)
	if err != nil {
		return nil, err
	}
	return &blostore{
		kv:    era,
		batch: batch,
	}, nil
}
