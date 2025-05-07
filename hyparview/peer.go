package hyparview

import (
	"github.com/tamararankovic/hyparview/transport"
	"github.com/tamararankovic/hyparview/data"
)

type Peer struct {
	node data.Node
	conn transport.Conn
}
