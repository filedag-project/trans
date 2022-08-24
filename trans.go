package trans

import (
	"fmt"
	"time"
)

var (
	ErrMsgFormat   = fmt.Errorf("message format broken")
	ErrTimeout     = fmt.Errorf("read or write timeout")
	ErrReplyFormat = fmt.Errorf("reply format broken")
	ErrNotFound    = fmt.Errorf("trans - key not found")
)

var (
	ReadHeaderTimeout  = time.Second * 2
	ReadBodyTimeout    = time.Second * 3
	WriteHeaderTimeout = time.Second * 2
	WriteBodyTimeout   = time.Second * 3
)
