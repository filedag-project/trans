package trans

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	kv "github.com/filedag-project/mutcask"
	logging "github.com/ipfs/go-log/v2"
)

var logger = logging.Logger("piecestore-trans")

const defaultConnNum = 1

const (
	targetActive = iota
	targetDown
)

type Client interface {
	kv.KVBasic
	kv.KVScanner
	kv.KVAdvance

	Close()
	TargetActive() bool
}

type payload struct {
	in  *Msg
	out chan *Reply
}

var _ Client = (*TransClient)(nil)

type TransClient struct {
	ctx         context.Context
	target      string
	targetState int32
	connNum     int
	payloadChan chan *payload
	closeChan   chan struct{}
	close       func()
}

func NewTransClient(ctx context.Context, target string, connNum int) *TransClient {
	if connNum <= 0 {
		connNum = defaultConnNum
	}

	tra := &TransClient{
		ctx:         ctx,
		target:      target,
		targetState: targetActive,
		connNum:     connNum,
		payloadChan: make(chan *payload),
		closeChan:   make(chan struct{}),
	}
	var once sync.Once
	tra.close = func() {
		once.Do(func() {
			close(tra.closeChan)
		})
	}
	tra.pingTarget()
	tra.initConns()
	return tra
}

func (tc *TransClient) initConns() {
	for i := 0; i < tc.connNum; i++ {
		go func(tc *TransClient) {
			var conn net.Conn
			var dialer = net.Dialer{
				Timeout: time.Second * 10,
			}
			var err error
			var idle time.Time = time.Now()
			var maxIdle = time.Second * 15
			defer func() {
				if conn != nil {
					conn.Close()
				}
			}()
			for {
				select {
				case <-tc.ctx.Done():
					return
				case <-tc.closeChan:
					return
				case p := <-tc.payloadChan:
					if tc.targetState == targetDown { // do nothing but report error when target is down
						logger.Errorf("failed to dail up: %s", tc.target)
						p.out <- &Reply{
							Code: rep_failed,
							Body: []byte(fmt.Sprintf("target: %s is down", tc.target)),
						}
						continue
					}

					if conn == nil {
						conn, err = dialer.Dial("tcp", tc.target)
						if err != nil {
							logger.Errorf("failed to open stream: %s", err)
							p.out <- &Reply{
								Code: rep_failed,
								Body: []byte(fmt.Sprintf("failed to connect to target: %s", tc.target)),
							}
							continue
						}
						logger.Info("conn created!")
					}
					switch p.in.Act {
					case act_get:
						fallthrough
					case act_size:
						fallthrough
					case act_del:
						fallthrough
					case act_checksum:
						fallthrough
					case act_put:
						err = tc.send(&conn, p, dialer, time.Since(idle) > maxIdle)
						if err != nil {
							if conn != nil {
								conn.Close()
								conn = nil
							}
						}
					default:
						logger.Warnf("client does not support %s yet", p.in.Act)
						p.out <- &Reply{
							Code: rep_failed,
							Body: []byte("unsupported action"),
						}
					}
					idle = time.Now()
					logger.Infof("idle start: %v", idle)
				}
			}

		}(tc)
	}
}

func (tc *TransClient) Size(key []byte) (int, error) {
	msg := &Msg{
		Act: act_size,
		Key: key,
	}
	ch := make(chan *Reply)
	tc.payloadChan <- &payload{
		in:  msg,
		out: ch,
	}
	reply := <-ch
	//logger.Warnf("size: %v", reply.Body)
	defer vBuf.Put(&reply.Body)

	if reply.Code == rep_failed {
		return -1, fmt.Errorf("%s", reply.Body)
	}
	if reply.Code == rep_nofound {
		return -1, ErrNotFound
	}
	size, err := strconv.ParseInt(string(reply.Body), 10, 32)
	if err != nil {
		return -1, fmt.Errorf("failed to parse body into size: %s, %s", reply.Body, err)
	}
	return int(size), nil
}

func (tc *TransClient) Has(key []byte) (bool, error) {
	if _, err := tc.Size(key); err != nil {
		return false, nil
	}
	return true, nil
}

func (tc *TransClient) Delete(key []byte) error {
	msg := &Msg{
		Act: act_del,
		Key: key,
	}
	ch := make(chan *Reply)
	tc.payloadChan <- &payload{
		in:  msg,
		out: ch,
	}
	reply := <-ch
	defer vBuf.Put(&reply.Body)
	if reply.Code == rep_failed {
		return fmt.Errorf("%s", reply.Body)
	}

	return nil
}

func (tc *TransClient) Get(key []byte) ([]byte, error) {
	msg := &Msg{
		Act: act_get,
		Key: key,
	}
	ch := make(chan *Reply)
	tc.payloadChan <- &payload{
		in:  msg,
		out: ch,
	}
	reply := <-ch
	defer vBuf.Put(&reply.Body)
	if reply.Code == rep_failed {
		return nil, fmt.Errorf("%s", reply.Body)
	}
	if reply.Code == rep_nofound {
		return nil, ErrNotFound
	}
	if err := msg.FromBytes(reply.Body); err != nil {
		return nil, err
	}
	defer vBuf.Put(&msg.Value)
	if !bytes.Equal(msg.Key, key) {
		return nil, fmt.Errorf("reply key not match, expect: %s, got: %s", key, msg.Key)
	}
	v := make([]byte, len(msg.Value))
	copy(v, msg.Value)
	return v, nil
}

func (tc *TransClient) CheckSum(key []byte) (uint32, error) {
	msg := &Msg{
		Act: act_checksum,
		Key: key,
	}
	ch := make(chan *Reply)
	tc.payloadChan <- &payload{
		in:  msg,
		out: ch,
	}
	reply := <-ch
	defer vBuf.Put(&reply.Body)
	if reply.Code == rep_failed {
		return 0, fmt.Errorf("%s", reply.Body)
	}
	if reply.Code == rep_nofound {
		return 0, ErrNotFound
	}

	v := make([]byte, len(reply.Body))
	copy(v, reply.Body)
	checkSum, err := strconv.ParseUint(string(v), 10, 32)
	if err != nil {
		return 0, err
	}
	return uint32(checkSum), nil
}

func (tc *TransClient) Put(key []byte, value []byte) error {
	msg := &Msg{
		Act:   act_put,
		Key:   key,
		Value: value,
	}
	ch := make(chan *Reply)
	tc.payloadChan <- &payload{
		in:  msg,
		out: ch,
	}
	reply := <-ch
	//logger.Warnf("put: %v, len: %d, cap: %d", reply.Body, len(reply.Body), cap(reply.Body))
	defer vBuf.Put(&reply.Body)
	if reply.Code == rep_failed {
		return fmt.Errorf("%s", reply.Body)
	}
	return nil
}

func (tc *TransClient) AllKeysChan(context.Context) (chan string, error) {
	return nil, kv.ErrNotImpl
}

func (tc *TransClient) Scan(prefix []byte, max int) ([]kv.KVPair, error) {
	return nil, kv.ErrNotImpl
}

func (tc *TransClient) ScanKeys(prefix []byte, max int) ([][]byte, error) {
	return nil, kv.ErrNotImpl
}

func (tc *TransClient) send(connPtr *net.Conn, p *payload, dialer net.Dialer, exceedIdleTime bool) (err error) {
	retried := false
	conn := *connPtr
	logger.Infof("send msg: %s: %s, exceedIdleTime: %v", p.in.Act, p.in.Key, exceedIdleTime)
	defer func() {
		if err != nil {
			p.out <- &Reply{
				Code: rep_failed,
				Body: []byte(err.Error()),
			}
		}
		conn.SetDeadline(time.Time{})
	}()
	msg := p.in
START_SEND:
	conn.SetWriteDeadline(time.Now().Add(WriteHeaderTimeout))
	msgb := msg.Encode()
	defer vBuf.Put(&msgb)
	_, err = conn.Write(msgb)
	if err != nil {
		if retried || !exceedIdleTime {
			logger.Errorf("client %s: %s failed %s", p.in.Act, p.in.Key, err)
			return
		}
		newConn, e := dialer.Dial("tcp", tc.target)
		if e != nil {
			logger.Error("failed to dail up: ", e)
			err = e
			return
		}
		conn.Close()
		conn = newConn
		*connPtr = newConn
		goto START_SEND
	}
	buf := make([]byte, rephead_size)
	conn.SetReadDeadline(time.Now().Add(ReadHeaderTimeout))
	_, err = io.ReadFull(conn, buf)
	if err != nil {
		if retried || !exceedIdleTime {
			logger.Errorf("client %s: %s failed %s", p.in.Act, p.in.Key, err)
			return
		}
		// idle too long, maybe we need have to try more time and create a new conn
		newConn, e := dialer.Dial("tcp", tc.target)
		if e != nil {
			logger.Error("failed to dail up: ", e)
			err = e
			return
		}
		conn.Close()
		conn = newConn
		*connPtr = newConn
		goto START_SEND
	}
	h, err := ReplyHeadFrom(buf)
	if err != nil {
		return
	}
	// buf2 := make([]byte, h.BodySize)
	buf2ref := vBuf.Get().(*[]byte)
	(*buffer)(buf2ref).size(int(h.BodySize))
	buf2 := *buf2ref

	conn.SetReadDeadline(time.Now().Add(ReadBodyTimeout))
	_, err = io.ReadFull(conn, buf2)
	if err != nil {
		return
	}
	rep := &Reply{}
	rep.From(h, buf2)
	p.out <- rep
	return
}

func (tc *TransClient) Close() {
	tc.close()
}

func (tc *TransClient) pingTarget() {
	go func(tc *TransClient) {
		var conn net.Conn
		var dialer = net.Dialer{
			Timeout: time.Second * 10,
		}
		var err error
		ticker := time.NewTicker(time.Second * 2)
		defer func() {
			if conn != nil {
				conn.Close()
			}
		}()
		for {
			select {
			case <-tc.ctx.Done():
				return
			case <-tc.closeChan:
				return
			case <-ticker.C:
				if conn == nil {
					conn, err = dialer.Dial("tcp", tc.target)
					if err != nil {
						tc.setConnStatus(false)
						continue
					} else {
						tc.setConnStatus(true)
					}
				}
				if err := tc.ping(conn); err != nil {
					conn.Close()
					conn = nil
					tc.setConnStatus(false)
				}
			}
		}

	}(tc)
}

func (tc *TransClient) setConnStatus(up bool) {
	if up {
		atomic.CompareAndSwapInt32(&tc.targetState, targetDown, targetActive)
		logger.Infof("ping - set target state: %d", tc.targetState)
	} else {
		atomic.CompareAndSwapInt32(&tc.targetState, targetActive, targetDown)
		logger.Errorf("ping - failed to dail up: %s", tc.target)
		logger.Infof("ping - set target state: %d", tc.targetState)
	}
}

func (tc *TransClient) ping(conn net.Conn) (err error) {
	logger.Infof("ping start")

	msg := &Msg{
		Act: act_ping,
	}

	conn.SetWriteDeadline(time.Now().Add(WriteHeaderTimeout))
	msgb := msg.Encode()
	defer vBuf.Put(&msgb)
	_, err = conn.Write(msgb)
	if err != nil {
		logger.Errorf("client ping write msg failed %s", err)
		return
	}
	buf := make([]byte, rephead_size)

	conn.SetReadDeadline(time.Now().Add(ReadHeaderTimeout))
	_, err = io.ReadFull(conn, buf)
	if err != nil {
		logger.Errorf("client ping read head failed %s", err)
		return

	}
	h, err := ReplyHeadFrom(buf)
	if err != nil {
		return
	}

	buf = make([]byte, h.BodySize)

	conn.SetReadDeadline(time.Now().Add(ReadBodyTimeout))
	_, err = io.ReadFull(conn, buf)
	if err != nil {
		return
	}
	if err := msg.FromBytes(buf); err == nil {
		logger.Infof("received: %s", msg.Act)
	}
	//vBuf.Put(&msg.Value)
	logger.Infof("ping end")
	return nil
}

func (tc *TransClient) TargetActive() bool {
	return tc.targetState == targetActive
}
