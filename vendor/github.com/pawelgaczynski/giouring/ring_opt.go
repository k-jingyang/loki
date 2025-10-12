package giouring

import "time"

func WithSQPolling() ringOpt {
	return func(p *Params) {
		p.flags |= SetupSQPoll
	}
}

func WithSQThreadIdle(idle time.Duration) ringOpt {
	return func(p *Params) {
		p.sqThreadIdle = uint32(idle.Milliseconds())
	}
}
