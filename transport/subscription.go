package transport

type Subscription struct {
	unsub chan struct{}
}

func (s Subscription) Unsubscribe() {
	s.unsub <- struct{}{}
}

func Subscribe[T any](ch chan T, handler func(peer T)) Subscription {
	unsub := make(chan struct{})
	go func() {
		for {
			select {
			case peer := <-ch:
				handler(peer)
			case <-unsub:
				return
			}
		}
	}()
	return Subscription{unsub: unsub}
}
