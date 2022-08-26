package trans

import (
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"github.com/xtaci/kcp-go/v5"
)

var logger = logging.Logger("piecestore-trans")

const defaultConnNum = 1
const defaultConnForAllKeysChan = 3

const (
	targetActive = iota
	targetDown
)

type Client interface {
	Size(string) (int, error)
	Has(string) (bool, error)
	Delete(string) error
	Get(string) ([]byte, error)
	Put(string, []byte) error
	AllKeysChan(string) (chan string, error)
	Close()
	TargetActive() bool
	CheckSum(string) (string, error)
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
	allKeysChan chan *payload
	closeChan   chan struct{}
	close       func()
	protocol    string
}

func NewTransClient(ctx context.Context, target string, connNum int, protocol string) *TransClient {
	if connNum <= 0 {
		connNum = defaultConnNum
	}
	tra := &TransClient{
		ctx:         ctx,
		target:      target,
		connNum:     connNum,
		payloadChan: make(chan *payload),
		allKeysChan: make(chan *payload),
		closeChan:   make(chan struct{}),
		protocol:    protocol,
	}
	var once sync.Once
	tra.close = func() {
		once.Do(func() {
			close(tra.closeChan)
		})
	}
	tra.pingTarget()
	tra.initConns()
	tra.servAllKeysChan()
	return tra
}

func (tc *TransClient) dial(target string) (net.Conn, error) {
	if tc.protocol == "kcp" {
		return kcp.Dial(target)
	}
	if tc.protocol == "tcp" {
		dialer := net.Dialer{
			Timeout: time.Second * 10,
		}
		return dialer.Dial("tcp", target)
	}
	return nil, fmt.Errorf("unsupported protocol: %s", tc.protocol)
}

func (tc *TransClient) initConns() {
	for i := 0; i < tc.connNum; i++ {
		go func(tc *TransClient) {
			var conn net.Conn
			// var dialer = net.Dialer{
			// 	Timeout: time.Second * 10,
			// }
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
						if conn, err = tc.dial(tc.target); err != nil {
							logger.Errorf("failed to dail up: %s, %s", tc.target, err)
							p.out <- &Reply{
								Code: rep_failed,
								Body: []byte(err.Error()),
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
						err = tc.send(&conn, p, time.Since(idle) > maxIdle)
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

func (tc *TransClient) servAllKeysChan() {
	for i := 0; i < defaultConnForAllKeysChan; i++ {
		go func(tc *TransClient) {
			for {
				select {
				case <-tc.ctx.Done():
					return
				case <-tc.closeChan:
					return
				case p := <-tc.allKeysChan:
					func() {
						if tc.targetState == targetDown { // do nothing but report error when target is down
							logger.Errorf("failed to dail up: %s", tc.target)
							p.out <- &Reply{
								Code: rep_failed,
								Body: []byte(fmt.Sprintf("target: %s is down", tc.target)),
							}
							return
						}

						conn, err := tc.dial(tc.target)
						if err != nil {
							logger.Errorf("failed to dail up: %s, %s", tc.target, err)
							p.out <- &Reply{
								Code: rep_failed,
								Body: []byte(err.Error()),
							}
							return
						}
						defer conn.Close()

						switch p.in.Act {
						case act_get_keys:
							tc.sendGetKeys(conn, p)
						default:
							logger.Warnf("allKeysChan does not support %s yet", p.in.Act)
							p.out <- &Reply{
								Code: rep_failed,
								Body: []byte("unsupported action"),
							}
						}
					}()
				}
			}

		}(tc)
	}
}

func (tc *TransClient) Size(key string) (int, error) {
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

func (tc *TransClient) Has(key string) (bool, error) {
	if _, err := tc.Size(key); err != nil {
		return false, nil
	}
	return true, nil
}

func (tc *TransClient) Delete(key string) error {
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

func (tc *TransClient) Get(key string) ([]byte, error) {
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
	if msg.Key != key {
		return nil, fmt.Errorf("reply key not match, expect: %s, got: %s", key, msg.Key)
	}
	v := make([]byte, len(msg.Value))
	copy(v, msg.Value)
	return v, nil
}

func (tc *TransClient) CheckSum(key string) (string, error) {
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
		return "", fmt.Errorf("%s", reply.Body)
	}
	if reply.Code == rep_nofound {
		return "", ErrNotFound
	}

	v := make([]byte, len(reply.Body))
	copy(v, reply.Body)
	return string(v), nil
}

func (tc *TransClient) Put(key string, value []byte) error {
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

func (tc *TransClient) AllKeysChan(startKey string) (chan string, error) {
	kc := make(chan string)
	go func(tc *TransClient, kc chan string) {
		defer close(kc)
		msg := &Msg{
			Act: act_get_keys,
			Key: startKey,
		}
		ch := make(chan *Reply)
		tc.allKeysChan <- &payload{
			in:  msg,
			out: ch,
		}

		for reply := range ch {
			if reply.Code == rep_failed {
				if string(reply.Body) != "EOF" {
					logger.Errorf("%s", reply.Body)
				}
				return
			}
			k := make([]byte, len(reply.Body))
			copy(k, reply.Body)
			shortBuf.Put(&reply.Body)
			kc <- string(k)
		}
	}(tc, kc)
	return kc, nil
}

func (tc *TransClient) send(connPtr *net.Conn, p *payload, exceedIdleTime bool) (err error) {
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
		newConn, e := tc.dial(tc.target)
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
	//buf := make([]byte, rephead_size)
	buf := shortBuf.Get().(*[]byte)
	(*buffer)(buf).size(rephead_size)
	defer shortBuf.Put(buf)
	conn.SetReadDeadline(time.Now().Add(ReadHeaderTimeout))
	_, err = io.ReadFull(conn, *buf)
	if err != nil {
		if retried || !exceedIdleTime {
			logger.Errorf("client %s: %s failed %s", p.in.Act, p.in.Key, err)
			return
		}
		// idle too long, maybe we need have to try more time and create a new conn
		newConn, e := tc.dial(tc.target)
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
	h, err := ReplyHeadFrom(*buf)
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

func (tc *TransClient) sendGetKeys(conn net.Conn, p *payload) {
	var err error
	logger.Infof("send msg: %s: %s, exceedIdleTime: %v", p.in.Act, p.in.Key)
	defer func() {
		if err != nil {
			p.out <- &Reply{
				Code: rep_failed,
				Body: []byte(err.Error()),
			}
		}
		close(p.out)
	}()
	msg := p.in
	conn.SetWriteDeadline(time.Now().Add(WriteHeaderTimeout))
	msgb := msg.Encode()
	defer vBuf.Put(&msgb)
	_, err = conn.Write(msgb)
	if err != nil {
		logger.Errorf("client %s: %s failed %s", p.in.Act, p.in.Key, err)
		return
	}
	for {
		select {
		case <-tc.ctx.Done():
			err = tc.ctx.Err()
			return
		case <-tc.closeChan:
			err = fmt.Errorf("client has been closed")
		default:
			if err := func() error {
				//buf := make([]byte, rephead_size)
				conn.Write([]byte{1})
				buf := shortBuf.Get().(*[]byte)
				defer shortBuf.Put(buf)
				(*buffer)(buf).size(rephead_size)
				conn.SetReadDeadline(time.Now().Add(ReadHeaderTimeout))
				_, err = io.ReadFull(conn, *buf)
				if err != nil {
					if err != io.EOF {
						logger.Errorf("client %s: %s failed %s", p.in.Act, p.in.Key, err)
					}
					return err
				}
				h, err := ReplyHeadFrom(*buf)
				if err != nil {
					return err
				}
				//buf = make([]byte, h.BodySize)
				(*buffer)(buf).size(int(h.BodySize))
				conn.SetReadDeadline(time.Now().Add(ReadBodyTimeout))
				_, err = io.ReadFull(conn, *buf)
				if err != nil {
					return err
				}
				rep := &Reply{}
				rep.From(h, *buf)
				if string(rep.Body) == "EOF" {
					logger.Warnf("reach EOF")
					return fmt.Errorf("EOF")
				}
				p.out <- rep
				return nil
			}(); err != nil {
				return
			}
		}
	}
}

func (tc *TransClient) Close() {
	tc.close()
}

func (tc *TransClient) pingTarget() {
	go func(tc *TransClient) {
		var conn net.Conn
		var err error
		ticker := time.NewTicker(time.Second * 3)
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
					if conn, err = tc.dial(tc.target); err != nil {
						atomic.CompareAndSwapInt32(&tc.targetState, targetActive, targetDown)
						logger.Errorf("ping - failed to dail up: %s", tc.target)
						logger.Infof("ping - set target state: %d", tc.targetState)
						continue
					} else {
						atomic.CompareAndSwapInt32(&tc.targetState, targetDown, targetActive)
						logger.Infof("ping - set target state: %d", tc.targetState)
					}
				}
				if err := tc.ping(conn); err != nil {
					conn.Close()
					conn = nil
				}
			}
		}

	}(tc)
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
	// buf := make([]byte, rephead_size)
	buf := shortBuf.Get().(*[]byte)
	(*buffer)(buf).size(rephead_size)
	defer shortBuf.Put(buf)
	conn.SetReadDeadline(time.Now().Add(ReadHeaderTimeout))
	_, err = io.ReadFull(conn, *buf)
	if err != nil {
		logger.Errorf("client ping read head failed %s", err)
		return

	}
	h, err := ReplyHeadFrom(*buf)
	if err != nil {
		return
	}
	buf2 := shortBuf.Get().(*[]byte)
	(*buffer)(buf2).size(int(h.BodySize))
	defer shortBuf.Put(buf2)
	// buf = make([]byte, h.BodySize)
	// buf2 := &buf
	conn.SetReadDeadline(time.Now().Add(ReadBodyTimeout))
	_, err = io.ReadFull(conn, *buf2)
	if err != nil {
		return
	}
	if err := msg.FromBytes(*buf2); err == nil {
		logger.Infof("received: %s", msg.Act)
	}
	//vBuf.Put(&msg.Value)
	logger.Infof("ping end")
	return nil
}

func (tc *TransClient) TargetActive() bool {
	return tc.targetState == targetActive
}
