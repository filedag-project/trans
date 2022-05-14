package trans

import (
	"bytes"
	"testing"
)

var msglist = []*Msg{
	{act_get, "piecestore_message_01", nil},
	{act_put, "piecestore_message_01", []byte("data_for_message_01")},
	{act_del, "piecestore_message_02", nil},
	{act_get_keys, "", nil},
	{act_conn_close, "", nil},
	{act_conn_keep, "", nil},
}

func TestMsg(t *testing.T) {
	for _, msg := range msglist {
		bs := msg.Encode()
		h, err := HeadFrom(bs[:header_size])
		if err != nil {
			t.Fatal("read head failed ", err)
		}
		if h.Act != msg.Act {
			t.Fatal("action not match ", h.Act, msg.Act)
		}
		msg2 := &Msg{}
		msg2.From(h, bs[header_size:])
		if msg2.Key != msg.Key {
			t.Fatal("key not match ", msg2.Key, msg.Key)
		}
		if !bytes.Equal(msg2.Value, msg.Value) {
			t.Fatal("value not match ", msg2.Value, msg.Value)
		}
		if err := msg2.FromBytes(bs); err != nil {
			t.Fatal(err)
		}
		if msg2.Act != msg.Act {
			t.Fatal("action not match ", msg2.Act, msg.Act)
		}
		if msg2.Key != msg.Key {
			t.Fatal("key not match ", msg2.Key, msg.Key)
		}
		if !bytes.Equal(msg2.Value, msg.Value) {
			t.Fatal("value not match ", msg2.Value, msg.Value)
		}
	}
}

var replylist = []*Reply{
	{rep_success, []byte("data_for_store.")},
	{rep_failed, []byte("this is a failed reply!")},
}

func TestReply(t *testing.T) {
	for _, msg := range replylist {
		bs := msg.Encode()
		h, err := ReplyHeadFrom(bs[:rephead_size])
		if err != nil {
			t.Fatal("read head failed ", err)
		}
		if h.Code != msg.Code {
			t.Fatal("code not match ", h.Code, msg.Code)
		}
		msg2 := &Reply{}
		msg2.From(h, bs[rephead_size:])

		if !bytes.Equal(msg2.Body, msg.Body) {
			t.Fatal("body not match ", msg2.Body, msg.Body)
		}

	}
}
