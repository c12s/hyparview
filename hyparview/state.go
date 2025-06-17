package hyparview

type State struct {
	ActiveView  []string
	PassiveView []string
	Left        bool
}

func (h *HyParView) GetState() any {
	// h.mu.Lock()
	// defer h.mu.Unlock()
	s := State{
		ActiveView:  make([]string, len(h.activeView.peers)),
		PassiveView: make([]string, len(h.passiveView.peers)),
		Left:        h.left,
	}
	for i, p := range h.activeView.peers {
		s.ActiveView[i] = p.Node.ID
	}
	for i, p := range h.passiveView.peers {
		s.PassiveView[i] = p.Node.ID
	}
	return s
}
