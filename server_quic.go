package trans

// import (
// 	"context"
// 	"crypto/rand"
// 	"crypto/rsa"
// 	"crypto/tls"
// 	"crypto/x509"
// 	"encoding/pem"
// 	"fmt"
// 	"io"
// 	"math/big"
// 	"sync"
// 	"time"

// 	kv "github.com/filedag-project/mutcask"
// 	"github.com/lucas-clemente/quic-go"
// )

// const QUIC_PROTOCOL = "trans-quic-proto"

// type QuicServ struct {
// 	ctx       context.Context
// 	kv        kv.KVStore
// 	addr      string // net listen address
// 	closeChan chan struct{}
// 	close     func()
// }

// func NewQuicServ(ctx context.Context, addr string, db kv.KVStore) (*QuicServ, error) {
// 	srv := &QuicServ{
// 		ctx:       ctx,
// 		kv:        db,
// 		addr:      addr,
// 		closeChan: make(chan struct{}),
// 	}
// 	var once sync.Once
// 	srv.close = func() {
// 		once.Do(func() {
// 			close(srv.closeChan)
// 		})
// 	}
// 	go srv.serv()
// 	return srv, nil
// }

// func (s *QuicServ) serv() {
// 	l, err := quic.ListenAddr(s.addr, generateTLSConfig(), nil)
// 	if err != nil {
// 		panic(err)
// 	}
// 	defer l.Close()
// 	for {
// 		select {
// 		case <-s.ctx.Done():
// 			return
// 		case <-s.closeChan:
// 			return
// 		default:
// 			conn, err := l.Accept(s.ctx)
// 			if err != nil {
// 				logger.Warnf("failed when accept connection: %s", err)
// 				continue
// 			}
// 			go func() {
// 				for {
// 					select {
// 					case <-s.ctx.Done():
// 						return
// 					case <-s.closeChan:
// 						return
// 					default:
// 						stream, err := conn.AcceptStream(s.ctx)
// 						if err != nil {
// 							logger.Warnf("failed when accept stream: %s", err)
// 							continue
// 						}
// 						go s.handleConnection(stream)
// 					}
// 				}

// 			}()

// 		}
// 	}
// }

// func (s *QuicServ) handleConnection(conn quic.Stream) {
// 	defer conn.Close()
// 	retry := 0
// 	max_retry := 1
// 	for {
// 		select {
// 		case <-s.ctx.Done():
// 			return
// 		case <-s.closeChan:
// 			return
// 		default:
// 			//buf := make([]byte, header_size)
// 			buf := headerBuf.Get().(*[]byte)
// 			conn.SetReadDeadline(time.Now().Add(ReadHeaderTimeout))
// 			n, err := io.ReadFull(conn, *buf)
// 			if err != nil {
// 				if err == io.EOF {
// 					logger.Info("handle conn: closed by peer")
// 					return
// 				}
// 				if retry >= max_retry {
// 					logger.Infof("handle conn: failed to read message header: %s", err)
// 					return
// 				}
// 				conn.SetDeadline(time.Time{})
// 				retry++
// 				//logger.Infof("handle conn: failed to read message header: %s\n wait annother round: %d", err, retry)
// 				continue
// 			}
// 			if n != header_size {
// 				logger.Errorf("handle conn: read header, expect %d bytes, got %d", header_size, n)
// 				return
// 			}
// 			h, err := HeadFrom(*buf)
// 			if err != nil {
// 				logger.Errorf("handle conn: failed to deserialize head: %s", err)
// 				return
// 			}
// 			headerBuf.Put(buf)
// 			logger.Infof("server receive message, action: %s", h.Act)
// 			switch h.Act {
// 			case act_conn_close:
// 				conn.SetDeadline(time.Time{}) // clear time out
// 				logger.Info("receive connection close message")
// 				return
// 			case act_ping:
// 				s.pong(conn)
// 				conn.SetDeadline(time.Time{}) // clear time out
// 				continue
// 			case act_get:
// 				if err := s.get(conn, h); err != nil {
// 					logger.Errorf("failed during get act: %s", err)
// 					if err == io.EOF {
// 						return
// 					}
// 				}
// 				conn.SetDeadline(time.Time{}) // clear time out
// 				continue
// 			case act_checksum:
// 				if err := s.checksum(conn, h); err != nil {
// 					logger.Errorf("failed during checksum act: %s", err)
// 					if err == io.EOF {
// 						return
// 					}
// 				}
// 				conn.SetDeadline(time.Time{}) // clear time out
// 				continue
// 			case act_put:
// 				if err := s.put(conn, h); err != nil {
// 					logger.Errorf("failed during put act: %s", err)
// 					if err == io.EOF {
// 						return
// 					}
// 				}
// 				conn.SetDeadline(time.Time{}) // clear time out
// 				continue
// 			case act_size:
// 				if err := s.size(conn, h); err != nil {
// 					logger.Errorf("failed during size act: %s", err)
// 					if err == io.EOF {
// 						return
// 					}
// 				}
// 				conn.SetDeadline(time.Time{}) // clear time out
// 				continue
// 			case act_del:
// 				if err := s.delete(conn, h); err != nil {
// 					logger.Errorf("failed during delete act: %s", err)
// 					if err == io.EOF {
// 						return
// 					}
// 				}
// 				conn.SetDeadline(time.Time{}) // clear time out
// 				continue
// 			case act_get_keys:
// 				conn.SetDeadline(time.Time{}) // clear time out
// 				s.allKeys(conn, h)
// 				return
// 			default:
// 				logger.Warnf("unknown action: %d\n", h.Act)
// 				return
// 			}
// 		}
// 	}
// }

// func (s *QuicServ) checksum(conn quic.Stream, h *Head) error {
// 	// buf := make([]byte, h.KSize+h.VSize)
// 	bufref := vBuf.Get().(*[]byte)
// 	(*buffer)(bufref).size(int(h.KSize + h.VSize))
// 	defer vBuf.Put(bufref)
// 	buf := *bufref
// 	conn.SetReadDeadline(time.Now().Add(ReadBodyTimeout))
// 	n, err := io.ReadFull(conn, buf)
// 	if err != nil {
// 		return err
// 	}
// 	if n != len(buf) {
// 		return fmt.Errorf("read bytes not match expect %d, got %d", len(buf), n)
// 	}
// 	msg := &Msg{}
// 	msg.From(h, buf)
// 	defer vBuf.Put(&msg.Value)
// 	v, err := s.kv.CheckSum(msg.Key)
// 	reply := &Reply{}
// 	if err != nil {
// 		reply.Code = rep_failed
// 		if err == kv.ErrNotFound {
// 			reply.Code = rep_nofound
// 		}
// 		reply.Body = []byte(err.Error())
// 	} else {
// 		reply.Code = rep_success
// 		reply.Body = []byte(v)
// 	}

// 	conn.SetWriteDeadline(time.Now().Add(WriteBodyTimeout))
// 	if _, err := reply.DumpQuic(conn); err != nil {
// 		return err
// 	}
// 	return nil
// }

// func (s *QuicServ) get(conn quic.Stream, h *Head) error {
// 	// buf := make([]byte, h.KSize+h.VSize)
// 	bufref := vBuf.Get().(*[]byte)
// 	(*buffer)(bufref).size(int(h.KSize + h.VSize))
// 	defer vBuf.Put(bufref)
// 	buf := *bufref
// 	conn.SetReadDeadline(time.Now().Add(ReadBodyTimeout))
// 	n, err := io.ReadFull(conn, buf)
// 	if err != nil {
// 		return err
// 	}
// 	if n != len(buf) {
// 		return fmt.Errorf("read bytes not match expect %d, got %d", len(buf), n)
// 	}
// 	msg := &Msg{}
// 	msg.From(h, buf)
// 	defer vBuf.Put(&msg.Value)
// 	v, err := s.kv.Get(msg.Key)
// 	reply := &Reply{}
// 	if err != nil {
// 		reply.Code = rep_failed
// 		if err == kv.ErrNotFound {
// 			reply.Code = rep_nofound
// 		}
// 		reply.Body = []byte(err.Error())
// 	} else {
// 		msg.Value = v
// 		reply.Code = rep_success
// 		reply.Body = msg.Encode()
// 	}

// 	conn.SetWriteDeadline(time.Now().Add(WriteBodyTimeout))
// 	if _, err := reply.DumpQuic(conn); err != nil {
// 		return err
// 	}
// 	return nil
// }

// func (s *QuicServ) put(conn quic.Stream, h *Head) error {
// 	// buf := make([]byte, h.KSize+h.VSize)
// 	bufref := vBuf.Get().(*[]byte)
// 	(*buffer)(bufref).size(int(h.KSize + h.VSize))
// 	defer vBuf.Put(bufref)
// 	buf := *bufref
// 	conn.SetReadDeadline(time.Now().Add(ReadBodyTimeout))
// 	n, err := io.ReadFull(conn, buf)
// 	if err != nil {
// 		return err
// 	}
// 	if n != len(buf) {
// 		return fmt.Errorf("read bytes not match expect %d, got %d", len(buf), n)
// 	}
// 	msg := &Msg{}
// 	msg.From(h, buf)
// 	defer vBuf.Put(&msg.Value)
// 	err = s.kv.Put(msg.Key, msg.Value)
// 	reply := &Reply{}
// 	if err != nil {
// 		reply.Code = rep_failed
// 		reply.Body = []byte(err.Error())
// 	} else {
// 		reply.Code = rep_success
// 	}

// 	conn.SetWriteDeadline(time.Now().Add(WriteBodyTimeout))
// 	if _, err := reply.DumpQuic(conn); err != nil {
// 		return err
// 	}
// 	return nil
// }

// func (s *QuicServ) size(conn quic.Stream, h *Head) error {
// 	// buf := make([]byte, h.KSize+h.VSize)
// 	bufref := vBuf.Get().(*[]byte)
// 	(*buffer)(bufref).size(int(h.KSize + h.VSize))
// 	defer vBuf.Put(bufref)
// 	buf := *bufref
// 	conn.SetReadDeadline(time.Now().Add(ReadBodyTimeout))
// 	n, err := io.ReadFull(conn, buf)
// 	if err != nil {
// 		return err
// 	}
// 	if n != len(buf) {
// 		return fmt.Errorf("read bytes not match expect %d, got %d", len(buf), n)
// 	}
// 	msg := &Msg{}
// 	msg.From(h, buf)
// 	defer vBuf.Put(&msg.Value)
// 	size, err := s.kv.Size(msg.Key)
// 	reply := &Reply{}
// 	if err != nil {
// 		reply.Code = rep_failed
// 		if err == kv.ErrNotFound {
// 			reply.Code = rep_nofound
// 		}
// 		reply.Body = []byte(err.Error())
// 	} else {
// 		reply.Code = rep_success
// 		reply.Body = []byte(fmt.Sprint(size))
// 	}

// 	conn.SetWriteDeadline(time.Now().Add(WriteBodyTimeout))
// 	if _, err := reply.DumpQuic(conn); err != nil {
// 		return err
// 	}
// 	return nil
// }

// func (s *QuicServ) delete(conn quic.Stream, h *Head) error {
// 	// buf := make([]byte, h.KSize+h.VSize)
// 	bufref := vBuf.Get().(*[]byte)
// 	(*buffer)(bufref).size(int(h.KSize + h.VSize))
// 	defer vBuf.Put(bufref)
// 	buf := *bufref
// 	conn.SetReadDeadline(time.Now().Add(ReadBodyTimeout))
// 	n, err := io.ReadFull(conn, buf)
// 	if err != nil {
// 		return err
// 	}
// 	if n != len(buf) {
// 		return fmt.Errorf("read bytes not match expect %d, got %d", len(buf), n)
// 	}
// 	msg := &Msg{}
// 	msg.From(h, buf)
// 	defer vBuf.Put(&msg.Value)
// 	err = s.kv.Delete(msg.Key)
// 	reply := &Reply{}
// 	if err != nil {
// 		reply.Code = rep_failed
// 		reply.Body = []byte(err.Error())
// 	} else {
// 		reply.Code = rep_success
// 	}

// 	conn.SetWriteDeadline(time.Now().Add(WriteBodyTimeout))
// 	if _, err := reply.DumpQuic(conn); err != nil {
// 		return err
// 	}
// 	return nil
// }

// func (s *QuicServ) pong(conn quic.Stream) error {
// 	msg := &Msg{
// 		Act: act_pong,
// 	}

// 	reply := &Reply{
// 		Code: rep_success,
// 		Body: msg.Encode(),
// 	}
// 	conn.SetWriteDeadline(time.Now().Add(WriteBodyTimeout))
// 	if _, err := reply.DumpQuic(conn); err != nil {
// 		return err
// 	}
// 	return nil
// }

// func (s *QuicServ) allKeys(conn quic.Stream, h *Head) {
// 	// buf := make([]byte, h.KSize+h.VSize)
// 	bufref := vBuf.Get().(*[]byte)
// 	(*buffer)(bufref).size(int(h.KSize + h.VSize))
// 	defer vBuf.Put(bufref)
// 	buf := *bufref
// 	conn.SetReadDeadline(time.Now().Add(ReadBodyTimeout))
// 	n, err := io.ReadFull(conn, buf)
// 	if err != nil {
// 		return
// 	}
// 	if n != len(buf) {
// 		logger.Errorf("read bytes not match expect %d, got %d", len(buf), n)
// 		return
// 	}
// 	// msg := &Msg{}
// 	// msg.From(h, buf)
// 	// vBuf.Put(buffer(msg.Value))

// 	kc, err := s.kv.AllKeysChan(s.ctx)
// 	if err != nil {
// 		reply := &Reply{}
// 		reply.Code = rep_failed
// 		reply.Body = []byte(err.Error())
// 		conn.SetWriteDeadline(time.Now().Add(WriteBodyTimeout))
// 		if _, err := reply.DumpQuic(conn); err != nil {
// 			return
// 		}
// 		return
// 	}

// 	for {
// 		select {
// 		case <-s.ctx.Done():
// 			return
// 		case <-s.closeChan:
// 			return
// 		case key := <-kc:
// 			if key == "" {
// 				return
// 			}
// 			reply := &Reply{}
// 			reply.Code = rep_success
// 			reply.Body = []byte(key)
// 			conn.SetWriteDeadline(time.Now().Add(WriteBodyTimeout))
// 			if _, err := reply.DumpQuic(conn); err != nil {
// 				return
// 			}
// 		}
// 	}
// }

// func (s *QuicServ) Close() {
// 	s.close()
// }

// // Setup a bare-bones TLS config for the server
// func generateTLSConfig() *tls.Config {
// 	key, err := rsa.GenerateKey(rand.Reader, 1024)
// 	if err != nil {
// 		panic(err)
// 	}
// 	template := x509.Certificate{SerialNumber: big.NewInt(1)}
// 	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
// 	if err != nil {
// 		panic(err)
// 	}
// 	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
// 	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})

// 	tlsCert, err := tls.X509KeyPair(certPEM, keyPEM)
// 	if err != nil {
// 		panic(err)
// 	}
// 	return &tls.Config{
// 		Certificates: []tls.Certificate{tlsCert},
// 		NextProtos:   []string{QUIC_PROTOCOL},
// 	}
// }
