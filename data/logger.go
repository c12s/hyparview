package data

import (
	"log"
	"os"
)

var LOG *log.Logger = log.New(os.Stdout, "hyparview", log.LstdFlags|log.Lshortfile)
