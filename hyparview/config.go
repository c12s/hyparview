package hyparview

type HyParViewConfig struct {
	Fanout,
	PassiveViewSize,
	ARWL,
	PRWL,
	ShuffleInterval,
	Ka,
	Kp int
}

type Config struct {
	NodeID,
	ListenAddress,
	ContactNodeID string
	ContactNodeAddress string
	HyParViewConfig
}
