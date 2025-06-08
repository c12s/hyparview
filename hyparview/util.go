package hyparview

import (
	"log"

	"github.com/c12s/hyparview/data"
)

func SetLogger(logger *log.Logger) {
	data.LOG = logger
}
