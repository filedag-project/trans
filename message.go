package trans

import (
	"encoding/binary"
	"net"
)

const action_size = 1
const key_size = 4
const value_size = 4
const header_size = action_size + key_size + value_size

type action byte

const (
	act_get action = iota
	act_put
	act_del
	act_size
	act_get_keys
	act_ping
	act_pong
	act_conn_keep
	act_conn_close
	act_checksum
	act_scan
	act_scan_keys
)

type Msg struct {
	Act   action
	Key   []byte
	Value []byte
}

type Head struct {
	Act   action
	KSize uint32
	VSize uint32
}

/**
		action		:	key size	:	value size	: 	key		:	value
			1   	:   	4   	:   	4		:	n		:	n
**/
func (m *Msg) Encode() []byte {
	klen := len(m.Key)
	vlen := len(m.Value)
	ret := make([]byte, header_size+klen+vlen)

	// action
	ret[0] = byte(m.Act)
	// key size
	binary.LittleEndian.PutUint32(ret[action_size:action_size+key_size], uint32(klen))
	// value size
	binary.LittleEndian.PutUint32(ret[action_size+key_size:header_size], uint32(vlen))
	// write key
	copy(ret[header_size:header_size+klen], m.Key)
	// write value
	copy(ret[header_size+klen:], m.Value)
	return ret
}

func (m *Msg) FromBytes(buf []byte) (err error) {
	if len(buf) < header_size {
		return ErrMsgFormat
	}
	h, _ := HeadFrom(buf[:header_size])

	m.From(h, buf[header_size:])
	return
}

// the buf here only contains bytes of key and value, no header bytes here
func (m *Msg) From(h *Head, buf []byte) {
	m.Act = h.Act
	// read key
	m.Key = make([]byte, h.KSize)
	copy(m.Key, buf[:h.KSize])
	// read value
	if m.Value == nil {
		m.Value = make([]byte, h.VSize)
	}
	copy(m.Value, buf[h.KSize:])
}

func HeadFrom(buf []byte) (h *Head, err error) {
	if len(buf) < header_size {
		return nil, ErrMsgFormat
	}
	h = &Head{}
	// read action
	h.Act = action(buf[0])
	// key size
	h.KSize = binary.LittleEndian.Uint32(buf[action_size : action_size+key_size])
	// value size
	h.VSize = binary.LittleEndian.Uint32(buf[action_size+key_size : header_size])
	return
}

func (act action) String() string {
	switch act {
	case act_get:
		return "get"
	case act_put:
		return "put"
	case act_del:
		return "delete"
	case act_size:
		return "size"
	case act_get_keys:
		return "get keys"
	case act_ping:
		return "ping"
	case act_pong:
		return "pong"
	case act_conn_keep:
		return "keep connection"
	case act_conn_close:
		return "close connection"
	case act_checksum:
		return "checksum"
	case act_scan:
		return "scan"
	case act_scan_keys:
		return "scan keys"
	default:
		return "unknown action"
	}
}

const repcode_size = 1
const repbody_size = 4
const rephead_size = repcode_size + repbody_size

type repcode byte

const (
	rep_success repcode = iota
	rep_failed
	rep_nofound
)

type Reply struct {
	Code repcode
	Body []byte
}

type ReplyHead struct {
	Code     repcode
	BodySize uint32
}

/**
		repcode		:	body size	:		body
			1   	:   	4   	:   	n
**/
func (r *Reply) Encode() []byte {
	blen := len(r.Body)

	ret := make([]byte, rephead_size+blen)
	// action
	ret[0] = byte(r.Code)
	// body size
	binary.LittleEndian.PutUint32(ret[repcode_size:rephead_size], uint32(blen))
	// write value
	copy(ret[rephead_size:], r.Body)
	return ret
}

// Caution: buf here only contains bytes of body
func (r *Reply) From(h *ReplyHead, buf []byte) {
	r.Code = h.Code
	r.Body = buf
}

func ReplyHeadFrom(buf []byte) (h *ReplyHead, err error) {
	if len(buf) < rephead_size {
		return nil, ErrReplyFormat
	}
	h = &ReplyHead{}
	// read repcode
	h.Code = repcode(buf[0])
	// body size
	h.BodySize = binary.LittleEndian.Uint32(buf[repcode_size:rephead_size])
	return
}

func (r *Reply) Dump(w net.Conn) (n int, err error) {
	blen := len(r.Body)
	ret := make([]byte, rephead_size+blen)
	// action
	ret[0] = byte(r.Code)
	// body size
	binary.LittleEndian.PutUint32(ret[repcode_size:rephead_size], uint32(blen))
	// write value
	copy(ret[rephead_size:], r.Body)
	return w.Write(ret)
}
