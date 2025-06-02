package hyparview

type HyParViewConfig struct {
	Fanout          int `env:"HV_FANOUT"`
	PassiveViewSize int `env:"HV_PASSIVE_VIEW_SIZE"`
	ARWL            int `env:"HV_ARWL"`
	PRWL            int `env:"HV_PRWL"`
	ShuffleInterval int `env:"HV_SHUFFLE_INTERVAL"`
	Ka              int `env:"HV_K_A"`
	Kp              int `env:"HV_K_P"`
}

type Config struct {
	NodeID             int64  `env:"NODE_ID"`
	ListenAddress      string `env:"LISTEN_ADDR"`
	ContactNodeID      int64  `env:"CONTACT_NODE_ID"`
	ContactNodeAddress string `env:"CONTACT_NODE_ADDR"`
	HyParViewConfig
}
