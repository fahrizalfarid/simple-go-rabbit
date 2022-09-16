package communication

type Connection interface {
	Connect() error
	BindQueue() error
	Publish(msg []byte) error
	RunConsumer()
	Close()
}

func NewConnection(cfg *Config) Connection {
	return &connection{
		uri:        cfg.Uri,
		queueName:  cfg.Queuename,
		disconnect: make(chan error, 1),
		timeoutCtx: cfg.TimeOutContext,
	}
}
