package trans

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"github.com/lucas-clemente/quic-go"
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
}

func NewTransClient(ctx context.Context, target string, connNum int) *TransClient {
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

func (tc *TransClient) initConns() {
	for i := 0; i < tc.connNum; i++ {
		go func(tc *TransClient) {
			tlsConf := &tls.Config{
				InsecureSkipVerify: true,
				NextProtos:         []string{QUIC_PROTOCOL},
			}

			var dialer quic.Connection
			var conn quic.Stream
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
					if dialer == nil {
						dialer, err = quic.DialAddr(tc.target, tlsConf, nil)
						if err != nil {
							logger.Errorf("failed to dail up: %s, %s", tc.target, err)
							continue
						}
					}
					if conn == nil {
						conn, err = dialer.OpenStreamSync(tc.ctx)
						if err != nil {
							logger.Errorf("failed to open stream: %s", err)
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
								dialer = nil
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
			tlsConf := &tls.Config{
				InsecureSkipVerify: true,
				NextProtos:         []string{QUIC_PROTOCOL},
			}
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
						dialer, err := quic.DialAddr(tc.target, tlsConf, nil)
						if err != nil {
							logger.Errorf("failed to dail up: %s, %s", tc.target, err)
							p.out <- &Reply{
								Code: rep_failed,
								Body: []byte(err.Error()),
							}
							return
						}

						conn, err := dialer.OpenStreamSync(tc.ctx)
						if err != nil {
							logger.Errorf("failed to open stream: %s", err)
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

func (tc *TransClient) send(connPtr *quic.Stream, p *payload, dialer quic.Connection, exceedIdleTime bool) (err error) {
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
		newConn, e := dialer.AcceptStream(tc.ctx)
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
		newConn, e := dialer.AcceptStream(tc.ctx)
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

func (tc *TransClient) sendGetKeys(conn quic.Stream, p *payload) {
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
		var dialer quic.Connection
		var conn quic.Stream

		tlsConf := &tls.Config{
			InsecureSkipVerify: true,
			NextProtos:         []string{QUIC_PROTOCOL},
		}
		var err error
		ticker := time.NewTicker(time.Second * 5)
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
				var dialFailed bool
				if dialer == nil {
					if dialer, err = quic.DialAddr(tc.target, tlsConf, nil); err != nil {
						dialFailed = true
					} else {
						if conn, err = dialer.AcceptStream(tc.ctx); err != nil {
							dialFailed = true
						} else {
							atomic.CompareAndSwapInt32(&tc.targetState, targetDown, targetActive)
							logger.Infof("ping - set target state: %d", tc.targetState)
						}
					}
					if dialFailed {
						atomic.CompareAndSwapInt32(&tc.targetState, targetActive, targetDown)
						logger.Errorf("ping - failed to dail up: %s", tc.target)
						logger.Infof("ping - set target state: %d", tc.targetState)
						continue
					}
				}
				if err := tc.ping(conn); err != nil {
					conn.Close()
					conn = nil
					dialer = nil
				}
			}
		}

	}(tc)
}

func (tc *TransClient) ping(conn quic.Stream) (err error) {
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
	(*buffer)(buf).size(repbody_size)
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
