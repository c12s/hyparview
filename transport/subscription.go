package transport

type Subscription struct {
	unsub chan struct{}
}

func (s Subscription) Unsubscribe() {
	s.unsub <- struct{}{}
}

func Subscribe[T any](ch chan T, handler func(T)) Subscription {
	unsub := make(chan struct{})
	go func() {
		for {
			select {
			case x := <-ch:
				if handler != nil {
					handler(x)
				}
			case <-unsub:
				return
			}
		}
	}()
	return Subscription{unsub: unsub}
}
