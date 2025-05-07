package hyparview

import (
	"github.com/c12s/hyparview/data"
	"github.com/c12s/hyparview/transport"
)

type Peer struct {
	node data.Node
	conn transport.Conn
}
