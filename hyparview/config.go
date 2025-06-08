package hyparview

type Config struct {
	Fanout          int `env:"HV_FANOUT"`
	PassiveViewSize int `env:"HV_PASSIVE_VIEW_SIZE"`
	ARWL            int `env:"HV_ARWL"`
	PRWL            int `env:"HV_PRWL"`
	ShuffleInterval int `env:"HV_SHUFFLE_INTERVAL"`
	Ka              int `env:"HV_K_A"`
	Kp              int `env:"HV_K_P"`
}