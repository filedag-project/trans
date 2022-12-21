package trans

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	kv "github.com/filedag-project/mutcask"
)

type PServ struct {
	ctx       context.Context
	kv        kv.KVStore
	addr      string // net listen address
	closeChan chan struct{}
	close     func()
}

func NewPServ(ctx context.Context, addr string, db kv.KVStore) (*PServ, error) {
	srv := &PServ{
		ctx:       ctx,
		kv:        db,
		addr:      addr,
		closeChan: make(chan struct{}),
	}
	var once sync.Once
	srv.close = func() {
		once.Do(func() {
			close(srv.closeChan)
		})
	}
	go srv.serv()
	return srv, nil
}

func (s *PServ) serv() {
	l, err := net.Listen("tcp", s.addr)
	if err != nil {
		panic(err)
	}
	defer l.Close()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-s.closeChan:
			return
		default:
			conn, err := l.Accept()
			if err != nil {
				panic(fmt.Errorf("failed when accept connection: %s", err))
			}
			go s.handleConnection(conn)
		}
	}
}

func (s *PServ) handleConnection(conn net.Conn) {
	defer conn.Close()
	retry := 0
	max_retry := 1
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-s.closeChan:
			return
		default:
			buf := make([]byte, header_size)
			conn.SetReadDeadline(time.Now().Add(ReadHeaderTimeout))
			n, err := io.ReadFull(conn, buf)
			if err != nil {
				if err == io.EOF {
					logger.Info("handle conn: closed by peer")
					return
				}
				if retry >= max_retry {
					logger.Infof("handle conn: failed to read message header: %s", err)
					return
				}
				conn.SetDeadline(time.Time{})
				retry++
				//logger.Infof("handle conn: failed to read message header: %s\n wait annother round: %d", err, retry)
				continue
			}
			if n != header_size {
				logger.Errorf("handle conn: read header, expect %d bytes, got %d", header_size, n)
				return
			}
			h, err := HeadFrom(buf)
			if err != nil {
				logger.Errorf("handle conn: failed to deserialize head: %s", err)
				return
			}
			logger.Infof("server receive message, action: %s", h.Act)
			switch h.Act {
			case act_conn_close:
				conn.SetDeadline(time.Time{}) // clear time out
				logger.Info("receive connection close message")
				return
			case act_ping:
				s.pong(conn)
				conn.SetDeadline(time.Time{}) // clear time out
				continue
			case act_get:
				if err := s.get(conn, h); err != nil {
					logger.Errorf("failed during get act: %s", err)
					if err == io.EOF {
						return
					}
				}
				conn.SetDeadline(time.Time{}) // clear time out
				continue
			case act_checksum:
				if err := s.checksum(conn, h); err != nil {
					logger.Errorf("failed during checksum act: %s", err)
					if err == io.EOF {
						return
					}
				}
				conn.SetDeadline(time.Time{}) // clear time out
				continue
			case act_put:
				if err := s.put(conn, h); err != nil {
					logger.Errorf("failed during put act: %s", err)
					if err == io.EOF {
						return
					}
				}
				conn.SetDeadline(time.Time{}) // clear time out
				continue
			case act_size:
				if err := s.size(conn, h); err != nil {
					logger.Errorf("failed during size act: %s", err)
					if err == io.EOF {
						return
					}
				}
				conn.SetDeadline(time.Time{}) // clear time out
				continue
			case act_del:
				if err := s.delete(conn, h); err != nil {
					logger.Errorf("failed during delete act: %s", err)
					if err == io.EOF {
						return
					}
				}
				conn.SetDeadline(time.Time{}) // clear time out
				continue
			case act_scan:
				if err := s.scan(conn, h); err != nil {
					logger.Errorf("failed during get act: %s", err)
					if err == io.EOF {
						return
					}
				}
				conn.SetDeadline(time.Time{}) // clear time out
				continue
			case act_scan_keys:
				if err := s.scanKeys(conn, h); err != nil {
					logger.Errorf("failed during get act: %s", err)
					if err == io.EOF {
						return
					}
				}
				conn.SetDeadline(time.Time{}) // clear time out
				continue
			case act_get_keys:
				conn.SetDeadline(time.Time{}) // clear time out
				s.allKeys(conn, h)
				return
			default:
				logger.Warnf("unknown action: %d\n", h.Act)
				return
			}
		}
	}
}

func (s *PServ) checksum(conn net.Conn, h *Head) error {
	buf := make([]byte, h.KSize+h.VSize)
	conn.SetReadDeadline(time.Now().Add(ReadBodyTimeout))
	n, err := io.ReadFull(conn, buf)
	if err != nil {
		return err
	}
	if n != len(buf) {
		return fmt.Errorf("read bytes not match expect %d, got %d", len(buf), n)
	}
	msg := &Msg{}
	msg.From(h, buf)

	v, err := s.kv.CheckSum(msg.Key)
	reply := &Reply{}
	if err != nil {
		reply.Code = rep_failed
		if err == kv.ErrNotFound {
			reply.Code = rep_nofound
		}
		reply.Body = []byte(err.Error())
	} else {
		reply.Code = rep_success
		reply.Body = []byte(fmt.Sprintf("%d", v))
	}

	conn.SetWriteDeadline(time.Now().Add(WriteBodyTimeout))
	if _, err := reply.Dump(conn); err != nil {
		return err
	}
	return nil
}

func (s *PServ) get(conn net.Conn, h *Head) error {
	buf := make([]byte, h.KSize+h.VSize)

	conn.SetReadDeadline(time.Now().Add(ReadBodyTimeout))
	n, err := io.ReadFull(conn, buf)
	if err != nil {
		return err
	}
	if n != len(buf) {
		return fmt.Errorf("read bytes not match expect %d, got %d", len(buf), n)
	}
	msg := &Msg{}
	msg.From(h, buf)
	v, err := s.kv.Get(msg.Key)
	reply := &Reply{}
	if err != nil {
		reply.Code = rep_failed
		if err == kv.ErrNotFound {
			reply.Code = rep_nofound
		}
		reply.Body = []byte(err.Error())
	} else {
		msg.Value = v
		reply.Code = rep_success
		reply.Body = msg.Encode()
	}

	conn.SetWriteDeadline(time.Now().Add(WriteBodyTimeout))
	if _, err := reply.Dump(conn); err != nil {
		return err
	}
	return nil
}

func (s *PServ) put(conn net.Conn, h *Head) error {
	buf := make([]byte, h.KSize+h.VSize)
	conn.SetReadDeadline(time.Now().Add(ReadBodyTimeout))
	n, err := io.ReadFull(conn, buf)
	if err != nil {
		return err
	}
	if n != len(buf) {
		return fmt.Errorf("read bytes not match expect %d, got %d", len(buf), n)
	}
	msg := &Msg{}
	msg.From(h, buf)
	err = s.kv.Put(msg.Key, msg.Value)
	reply := &Reply{}
	if err != nil {
		reply.Code = rep_failed
		reply.Body = []byte(err.Error())
	} else {
		reply.Code = rep_success
	}

	conn.SetWriteDeadline(time.Now().Add(WriteBodyTimeout))
	if _, err := reply.Dump(conn); err != nil {
		return err
	}
	return nil
}

func (s *PServ) size(conn net.Conn, h *Head) error {
	buf := make([]byte, h.KSize+h.VSize)
	conn.SetReadDeadline(time.Now().Add(ReadBodyTimeout))
	n, err := io.ReadFull(conn, buf)
	if err != nil {
		return err
	}
	if n != len(buf) {
		return fmt.Errorf("read bytes not match expect %d, got %d", len(buf), n)
	}
	msg := &Msg{}
	msg.From(h, buf)
	size, err := s.kv.Size(msg.Key)
	reply := &Reply{}
	if err != nil {
		reply.Code = rep_failed
		if err == kv.ErrNotFound {
			reply.Code = rep_nofound
		}
		reply.Body = []byte(err.Error())
	} else {
		reply.Code = rep_success
		reply.Body = []byte(fmt.Sprint(size))
	}

	conn.SetWriteDeadline(time.Now().Add(WriteBodyTimeout))
	if _, err := reply.Dump(conn); err != nil {
		return err
	}
	return nil
}

func (s *PServ) delete(conn net.Conn, h *Head) error {
	buf := make([]byte, h.KSize+h.VSize)
	conn.SetReadDeadline(time.Now().Add(ReadBodyTimeout))
	n, err := io.ReadFull(conn, buf)
	if err != nil {
		return err
	}
	if n != len(buf) {
		return fmt.Errorf("read bytes not match expect %d, got %d", len(buf), n)
	}
	msg := &Msg{}
	msg.From(h, buf)
	err = s.kv.Delete(msg.Key)
	reply := &Reply{}
	if err != nil {
		reply.Code = rep_failed
		reply.Body = []byte(err.Error())
	} else {
		reply.Code = rep_success
	}

	conn.SetWriteDeadline(time.Now().Add(WriteBodyTimeout))
	if _, err := reply.Dump(conn); err != nil {
		return err
	}
	return nil
}

func (s *PServ) scan(conn net.Conn, h *Head) error {
	buf := make([]byte, h.KSize+h.VSize)

	conn.SetReadDeadline(time.Now().Add(ReadBodyTimeout))
	n, err := io.ReadFull(conn, buf)
	if err != nil {
		return err
	}
	if n != len(buf) {
		return fmt.Errorf("read bytes not match expect %d, got %d", len(buf), n)
	}
	msg := &Msg{}
	msg.From(h, buf)
	max, err := b2i(msg.Value)
	if err != nil {
		return err
	}
	v, err := s.kv.Scan(msg.Key, max)
	reply := &Reply{}
	if err != nil {
		reply.Code = rep_failed
		reply.Body = []byte(err.Error())
	} else {
		bs, err := EncodeKVPair(v)
		if err != nil {
			reply.Code = rep_failed
			reply.Body = []byte(err.Error())
		} else {
			reply.Code = rep_success
			reply.Body = bs
		}
	}

	conn.SetWriteDeadline(time.Now().Add(WriteBodyTimeout))
	if _, err := reply.Dump(conn); err != nil {
		return err
	}
	return nil
}

func (s *PServ) scanKeys(conn net.Conn, h *Head) error {
	buf := make([]byte, h.KSize+h.VSize)

	conn.SetReadDeadline(time.Now().Add(ReadBodyTimeout))
	n, err := io.ReadFull(conn, buf)
	if err != nil {
		return err
	}
	if n != len(buf) {
		return fmt.Errorf("read bytes not match expect %d, got %d", len(buf), n)
	}
	msg := &Msg{}
	msg.From(h, buf)
	max, err := b2i(msg.Value)
	if err != nil {
		return err
	}
	v, err := s.kv.ScanKeys(msg.Key, max)
	reply := &Reply{}
	if err != nil {
		reply.Code = rep_failed
		reply.Body = []byte(err.Error())
	} else {
		bs, err := EncodeKeys(v)
		if err != nil {
			reply.Code = rep_failed
			reply.Body = []byte(err.Error())
		} else {
			reply.Code = rep_success
			reply.Body = bs
		}
	}

	conn.SetWriteDeadline(time.Now().Add(WriteBodyTimeout))
	if _, err := reply.Dump(conn); err != nil {
		return err
	}
	return nil
}

func (s *PServ) pong(conn net.Conn) error {
	msg := &Msg{
		Act: act_pong,
	}

	reply := &Reply{
		Code: rep_success,
		Body: msg.Encode(),
	}
	conn.SetWriteDeadline(time.Now().Add(WriteBodyTimeout))
	if _, err := reply.Dump(conn); err != nil {
		return err
	}
	return nil
}

func (s *PServ) allKeys(conn net.Conn, h *Head) {
	buf := make([]byte, h.KSize+h.VSize)

	conn.SetReadDeadline(time.Now().Add(ReadBodyTimeout))
	n, err := io.ReadFull(conn, buf)
	if err != nil {
		return
	}
	if n != len(buf) {
		logger.Errorf("read bytes not match expect %d, got %d", len(buf), n)
		return
	}
	// msg := &Msg{}
	// msg.From(h, buf)
	// vBuf.Put(buffer(msg.Value))

	kc, err := s.kv.AllKeysChan(s.ctx)
	if err != nil {
		reply := &Reply{}
		reply.Code = rep_failed
		reply.Body = []byte(err.Error())
		conn.SetWriteDeadline(time.Now().Add(WriteBodyTimeout))
		if _, err := reply.Dump(conn); err != nil {
			return
		}
		return
	}

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-s.closeChan:
			return
		case key := <-kc:
			if key == "" {
				return
			}
			reply := &Reply{}
			reply.Code = rep_success
			reply.Body = []byte(key)
			conn.SetWriteDeadline(time.Now().Add(WriteBodyTimeout))
			if _, err := reply.Dump(conn); err != nil {
				return
			}
		}
	}
}

func (s *PServ) Close() {
	s.close()
}
