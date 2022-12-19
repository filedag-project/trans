package trans

// var _ Client = (*QuicClient)(nil)

// type QuicClient struct {
// 	sync.Mutex
// 	ctx         context.Context
// 	target      string
// 	targetState int32
// 	connNum     int
// 	payloadChan chan *payload
// 	allKeysChan chan *payload
// 	closeChan   chan struct{}
// 	close       func()
// 	qc          quic.Connection
// }

// func NewQuicClient(ctx context.Context, target string, connNum int) *QuicClient {
// 	if connNum <= 0 {
// 		connNum = defaultConnNum
// 	}

// 	tra := &QuicClient{
// 		ctx:         ctx,
// 		target:      target,
// 		targetState: targetActive,
// 		connNum:     connNum,
// 		payloadChan: make(chan *payload),
// 		allKeysChan: make(chan *payload),
// 		closeChan:   make(chan struct{}),
// 	}
// 	var once sync.Once
// 	tra.close = func() {
// 		once.Do(func() {
// 			close(tra.closeChan)
// 		})
// 	}
// 	tra.dial()
// 	tra.pingTarget()
// 	tra.initConns()
// 	tra.servAllKeysChan()
// 	return tra
// }

// func (tc *QuicClient) newStream() (conn quic.Stream, err error) {
// 	if tc.qc == nil {
// 		return nil, fmt.Errorf("lost connection with target")
// 	}
// 	conn, err = tc.qc.OpenStreamSync(tc.ctx)
// 	return
// }

// func (tc *QuicClient) dial() (dialer quic.Connection, err error) {
// 	tlsConf := &tls.Config{
// 		InsecureSkipVerify: true,
// 		NextProtos:         []string{QUIC_PROTOCOL},
// 	}
// 	dialer, err = quic.DialAddr(tc.target, tlsConf, nil)
// 	if err != nil {
// 		logger.Warn(err)
// 		tc.setConnStatus(false)
// 		return
// 	}
// 	tc.Lock()
// 	tc.qc = dialer
// 	tc.Unlock()
// 	tc.setConnStatus(true)
// 	return
// }

// func (tc *QuicClient) initConns() {
// 	for i := 0; i < tc.connNum; i++ {
// 		go func(tc *QuicClient) {
// 			var conn quic.Stream
// 			var err error
// 			var idle time.Time = time.Now()
// 			var maxIdle = time.Second * 15
// 			defer func() {
// 				if conn != nil {
// 					conn.Close()
// 				}
// 			}()
// 			for {
// 				select {
// 				case <-tc.ctx.Done():
// 					return
// 				case <-tc.closeChan:
// 					return
// 				case p := <-tc.payloadChan:
// 					if tc.targetState == targetDown { // do nothing but report error when target is down
// 						logger.Errorf("failed to dail up: %s", tc.target)
// 						p.out <- &Reply{
// 							Code: rep_failed,
// 							Body: []byte(fmt.Sprintf("target: %s is down", tc.target)),
// 						}
// 						continue
// 					}

// 					if conn == nil {
// 						conn, err = tc.newStream()
// 						if err != nil {
// 							logger.Errorf("failed to open stream: %s", err)
// 							p.out <- &Reply{
// 								Code: rep_failed,
// 								Body: []byte(fmt.Sprintf("failed connect to target: %s", tc.target)),
// 							}
// 							continue
// 						}
// 						logger.Info("conn created!")
// 					}
// 					switch p.in.Act {
// 					case act_get:
// 						fallthrough
// 					case act_size:
// 						fallthrough
// 					case act_del:
// 						fallthrough
// 					case act_checksum:
// 						fallthrough
// 					case act_put:
// 						err = tc.send(&conn, p, time.Since(idle) > maxIdle)
// 						if err != nil {
// 							if conn != nil {
// 								conn.Close()
// 								conn = nil
// 							}
// 						}
// 					default:
// 						logger.Warnf("client does not support %s yet", p.in.Act)
// 						p.out <- &Reply{
// 							Code: rep_failed,
// 							Body: []byte("unsupported action"),
// 						}
// 					}
// 					idle = time.Now()
// 					logger.Infof("idle start: %v", idle)
// 				}
// 			}

// 		}(tc)
// 	}
// }

// func (tc *QuicClient) servAllKeysChan() {
// 	for i := 0; i < defaultConnForAllKeysChan; i++ {
// 		go func(tc *QuicClient) {
// 			for {
// 				select {
// 				case <-tc.ctx.Done():
// 					return
// 				case <-tc.closeChan:
// 					return
// 				case p := <-tc.allKeysChan:
// 					func() {
// 						if tc.targetState == targetDown { // do nothing but report error when target is down
// 							logger.Errorf("failed to dail up: %s", tc.target)
// 							p.out <- &Reply{
// 								Code: rep_failed,
// 								Body: []byte(fmt.Sprintf("target: %s is down", tc.target)),
// 							}
// 							return
// 						}

// 						conn, err := tc.newStream()
// 						if err != nil {
// 							logger.Errorf("failed to open stream: %s", err)
// 							p.out <- &Reply{
// 								Code: rep_failed,
// 								Body: []byte(err.Error()),
// 							}
// 							return
// 						}
// 						defer conn.Close()

// 						switch p.in.Act {
// 						case act_get_keys:
// 							tc.sendGetKeys(conn, p)
// 						default:
// 							logger.Warnf("allKeysChan does not support %s yet", p.in.Act)
// 							p.out <- &Reply{
// 								Code: rep_failed,
// 								Body: []byte("unsupported action"),
// 							}
// 						}
// 					}()
// 				}
// 			}

// 		}(tc)
// 	}
// }

// func (tc *QuicClient) Size(key string) (int, error) {
// 	msg := &Msg{
// 		Act: act_size,
// 		Key: key,
// 	}
// 	ch := make(chan *Reply)
// 	tc.payloadChan <- &payload{
// 		in:  msg,
// 		out: ch,
// 	}
// 	reply := <-ch
// 	//logger.Warnf("size: %v", reply.Body)
// 	defer vBuf.Put(&reply.Body)

// 	if reply.Code == rep_failed {
// 		return -1, fmt.Errorf("%s", reply.Body)
// 	}
// 	if reply.Code == rep_nofound {
// 		return -1, ErrNotFound
// 	}
// 	size, err := strconv.ParseInt(string(reply.Body), 10, 32)
// 	if err != nil {
// 		return -1, fmt.Errorf("failed to parse body into size: %s, %s", reply.Body, err)
// 	}
// 	return int(size), nil
// }

// func (tc *QuicClient) Has(key string) (bool, error) {
// 	if _, err := tc.Size(key); err != nil {
// 		return false, nil
// 	}
// 	return true, nil
// }

// func (tc *QuicClient) Delete(key string) error {
// 	msg := &Msg{
// 		Act: act_del,
// 		Key: key,
// 	}
// 	ch := make(chan *Reply)
// 	tc.payloadChan <- &payload{
// 		in:  msg,
// 		out: ch,
// 	}
// 	reply := <-ch
// 	defer vBuf.Put(&reply.Body)
// 	if reply.Code == rep_failed {
// 		return fmt.Errorf("%s", reply.Body)
// 	}

// 	return nil
// }

// func (tc *QuicClient) Get(key string) ([]byte, error) {
// 	msg := &Msg{
// 		Act: act_get,
// 		Key: key,
// 	}
// 	ch := make(chan *Reply)
// 	tc.payloadChan <- &payload{
// 		in:  msg,
// 		out: ch,
// 	}
// 	reply := <-ch
// 	defer vBuf.Put(&reply.Body)
// 	if reply.Code == rep_failed {
// 		return nil, fmt.Errorf("%s", reply.Body)
// 	}
// 	if reply.Code == rep_nofound {
// 		return nil, ErrNotFound
// 	}
// 	if err := msg.FromBytes(reply.Body); err != nil {
// 		return nil, err
// 	}
// 	defer vBuf.Put(&msg.Value)
// 	if msg.Key != key {
// 		return nil, fmt.Errorf("reply key not match, expect: %s, got: %s", key, msg.Key)
// 	}
// 	v := make([]byte, len(msg.Value))
// 	copy(v, msg.Value)
// 	return v, nil
// }

// func (tc *QuicClient) CheckSum(key string) (string, error) {
// 	msg := &Msg{
// 		Act: act_checksum,
// 		Key: key,
// 	}
// 	ch := make(chan *Reply)
// 	tc.payloadChan <- &payload{
// 		in:  msg,
// 		out: ch,
// 	}
// 	reply := <-ch
// 	defer vBuf.Put(&reply.Body)
// 	if reply.Code == rep_failed {
// 		return "", fmt.Errorf("%s", reply.Body)
// 	}
// 	if reply.Code == rep_nofound {
// 		return "", ErrNotFound
// 	}

// 	v := make([]byte, len(reply.Body))
// 	copy(v, reply.Body)
// 	return string(v), nil
// }

// func (tc *QuicClient) Put(key string, value []byte) error {
// 	msg := &Msg{
// 		Act:   act_put,
// 		Key:   key,
// 		Value: value,
// 	}
// 	ch := make(chan *Reply)
// 	tc.payloadChan <- &payload{
// 		in:  msg,
// 		out: ch,
// 	}
// 	reply := <-ch
// 	//logger.Warnf("put: %v, len: %d, cap: %d", reply.Body, len(reply.Body), cap(reply.Body))
// 	defer vBuf.Put(&reply.Body)
// 	if reply.Code == rep_failed {
// 		return fmt.Errorf("%s", reply.Body)
// 	}
// 	return nil
// }

// func (tc *QuicClient) AllKeysChan(startKey string) (chan string, error) {
// 	kc := make(chan string)
// 	go func(tc *QuicClient, kc chan string) {
// 		defer close(kc)
// 		msg := &Msg{
// 			Act: act_get_keys,
// 			Key: startKey,
// 		}
// 		ch := make(chan *Reply)
// 		tc.allKeysChan <- &payload{
// 			in:  msg,
// 			out: ch,
// 		}

// 		for reply := range ch {
// 			if reply.Code == rep_failed {
// 				if string(reply.Body) != "EOF" {
// 					logger.Errorf("%s", reply.Body)
// 				}
// 				return
// 			}
// 			k := make([]byte, len(reply.Body))
// 			copy(k, reply.Body)
// 			shortBuf.Put(&reply.Body)
// 			kc <- string(k)
// 		}
// 	}(tc, kc)
// 	return kc, nil
// }

// func (tc *QuicClient) send(connPtr *quic.Stream, p *payload, exceedIdleTime bool) (err error) {
// 	retried := false
// 	conn := *connPtr
// 	logger.Infof("send msg: %s: %s, exceedIdleTime: %v", p.in.Act, p.in.Key, exceedIdleTime)
// 	defer func() {
// 		if err != nil {
// 			p.out <- &Reply{
// 				Code: rep_failed,
// 				Body: []byte(err.Error()),
// 			}
// 		}
// 		conn.SetDeadline(time.Time{})
// 	}()
// 	msg := p.in
// START_SEND:
// 	conn.SetWriteDeadline(time.Now().Add(WriteHeaderTimeout))
// 	msgb := msg.Encode()
// 	defer vBuf.Put(&msgb)
// 	_, err = conn.Write(msgb)
// 	if err != nil {
// 		if retried || !exceedIdleTime {
// 			logger.Errorf("client %s: %s failed %s", p.in.Act, p.in.Key, err)
// 			return
// 		}
// 		newConn, e := tc.newStream()
// 		if e != nil {
// 			logger.Error("failed to dail up: ", e)
// 			err = e
// 			return
// 		}
// 		conn.Close()
// 		conn = newConn
// 		*connPtr = newConn
// 		goto START_SEND
// 	}
// 	//buf := make([]byte, rephead_size)
// 	buf := shortBuf.Get().(*[]byte)
// 	(*buffer)(buf).size(rephead_size)
// 	defer shortBuf.Put(buf)
// 	conn.SetReadDeadline(time.Now().Add(ReadHeaderTimeout))
// 	_, err = io.ReadFull(conn, *buf)
// 	if err != nil {
// 		if retried || !exceedIdleTime {
// 			logger.Errorf("client %s: %s failed %s", p.in.Act, p.in.Key, err)
// 			return
// 		}
// 		// idle too long, maybe we need have to try more time and create a new conn
// 		newConn, e := tc.newStream()
// 		if e != nil {
// 			logger.Error("failed to dail up: ", e)
// 			err = e
// 			return
// 		}
// 		conn.Close()
// 		conn = newConn
// 		*connPtr = newConn
// 		goto START_SEND
// 	}
// 	h, err := ReplyHeadFrom(*buf)
// 	if err != nil {
// 		return
// 	}
// 	// buf2 := make([]byte, h.BodySize)
// 	buf2ref := vBuf.Get().(*[]byte)
// 	(*buffer)(buf2ref).size(int(h.BodySize))
// 	buf2 := *buf2ref

// 	conn.SetReadDeadline(time.Now().Add(ReadBodyTimeout))
// 	_, err = io.ReadFull(conn, buf2)
// 	if err != nil {
// 		return
// 	}
// 	rep := &Reply{}
// 	rep.From(h, buf2)
// 	p.out <- rep
// 	return
// }

// func (tc *QuicClient) sendGetKeys(conn quic.Stream, p *payload) {
// 	var err error
// 	logger.Infof("send msg: %s: %s", p.in.Act, p.in.Key)
// 	defer func() {
// 		if err != nil {
// 			p.out <- &Reply{
// 				Code: rep_failed,
// 				Body: []byte(err.Error()),
// 			}
// 		}
// 		close(p.out)
// 	}()
// 	msg := p.in
// 	conn.SetWriteDeadline(time.Now().Add(WriteHeaderTimeout))
// 	msgb := msg.Encode()
// 	defer vBuf.Put(&msgb)
// 	_, err = conn.Write(msgb)
// 	if err != nil {
// 		logger.Errorf("client %s: %s failed %s", p.in.Act, p.in.Key, err)
// 		return
// 	}
// 	for {
// 		select {
// 		case <-tc.ctx.Done():
// 			err = tc.ctx.Err()
// 			return
// 		case <-tc.closeChan:
// 			err = fmt.Errorf("client has been closed")
// 		default:
// 			if err := func() error {
// 				//buf := make([]byte, rephead_size)
// 				buf := shortBuf.Get().(*[]byte)
// 				defer shortBuf.Put(buf)
// 				(*buffer)(buf).size(rephead_size)
// 				conn.SetReadDeadline(time.Now().Add(ReadHeaderTimeout))
// 				_, err = io.ReadFull(conn, *buf)
// 				if err != nil {
// 					if err != io.EOF {
// 						logger.Errorf("client %s: %s failed %s", p.in.Act, p.in.Key, err)
// 					}
// 					return err
// 				}
// 				h, err := ReplyHeadFrom(*buf)
// 				if err != nil {
// 					return err
// 				}
// 				//buf = make([]byte, h.BodySize)
// 				(*buffer)(buf).size(int(h.BodySize))
// 				conn.SetReadDeadline(time.Now().Add(ReadBodyTimeout))
// 				_, err = io.ReadFull(conn, *buf)
// 				if err != nil {
// 					return err
// 				}
// 				rep := &Reply{}
// 				rep.From(h, *buf)
// 				p.out <- rep
// 				return nil
// 			}(); err != nil {
// 				return
// 			}
// 		}
// 	}
// }

// func (tc *QuicClient) Close() {
// 	tc.close()
// }

// func (tc *QuicClient) pingTarget() {
// 	go func(tc *QuicClient) {
// 		var conn quic.Stream
// 		var err error
// 		ticker := time.NewTicker(time.Second * 2)
// 		defer func() {
// 			if conn != nil {
// 				conn.Close()
// 			}
// 		}()
// 		for {
// 			select {
// 			case <-tc.ctx.Done():
// 				return
// 			case <-tc.closeChan:
// 				return
// 			case <-ticker.C:
// 				if conn == nil {
// 					if tc.qc == nil {
// 						// try to reconnect
// 						tc.dial()
// 					}
// 					if tc.qc == nil {
// 						continue
// 					}
// 					conn, err = tc.qc.OpenStreamSync(tc.ctx)
// 					if err != nil {
// 						// maybe lost connect with server, so set tc.qc to nil
// 						tc.Lock()
// 						tc.qc = nil
// 						tc.Unlock()
// 						tc.setConnStatus(false)
// 						continue
// 					}
// 				}
// 				if err := tc.ping(conn); err != nil {
// 					conn.Close()
// 					conn = nil
// 					tc.setConnStatus(false)
// 				}
// 			}
// 		}

// 	}(tc)
// }

// func (tc *QuicClient) setConnStatus(up bool) {
// 	if up {
// 		atomic.CompareAndSwapInt32(&tc.targetState, targetDown, targetActive)
// 		logger.Infof("ping - set target state: %d", tc.targetState)
// 	} else {
// 		atomic.CompareAndSwapInt32(&tc.targetState, targetActive, targetDown)
// 		logger.Errorf("ping - failed to dail up: %s", tc.target)
// 		logger.Infof("ping - set target state: %d", tc.targetState)
// 	}
// }

// func (tc *QuicClient) ping(conn quic.Stream) (err error) {
// 	logger.Infof("ping start")

// 	msg := &Msg{
// 		Act: act_ping,
// 	}

// 	conn.SetWriteDeadline(time.Now().Add(WriteHeaderTimeout))
// 	msgb := msg.Encode()
// 	defer vBuf.Put(&msgb)
// 	_, err = conn.Write(msgb)
// 	if err != nil {
// 		logger.Errorf("client ping write msg failed %s", err)
// 		return
// 	}
// 	// buf := make([]byte, rephead_size)
// 	buf := shortBuf.Get().(*[]byte)
// 	(*buffer)(buf).size(rephead_size)
// 	defer shortBuf.Put(buf)
// 	conn.SetReadDeadline(time.Now().Add(ReadHeaderTimeout))
// 	_, err = io.ReadFull(conn, *buf)
// 	if err != nil {
// 		logger.Errorf("client ping read head failed %s", err)
// 		return

// 	}
// 	h, err := ReplyHeadFrom(*buf)
// 	if err != nil {
// 		return
// 	}
// 	buf2 := shortBuf.Get().(*[]byte)
// 	(*buffer)(buf2).size(int(h.BodySize))
// 	defer shortBuf.Put(buf2)
// 	// buf = make([]byte, h.BodySize)
// 	// buf2 := &buf
// 	conn.SetReadDeadline(time.Now().Add(ReadBodyTimeout))
// 	_, err = io.ReadFull(conn, *buf2)
// 	if err != nil {
// 		return
// 	}
// 	if err := msg.FromBytes(*buf2); err == nil {
// 		logger.Infof("received: %s", msg.Act)
// 	}
// 	//vBuf.Put(&msg.Value)
// 	logger.Infof("ping end")
// 	return nil
// }

// func (tc *QuicClient) TargetActive() bool {
// 	return tc.targetState == targetActive
// }
