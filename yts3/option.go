package yts3

import "time"

type Option func(g *Yts3)

func WithTimeSource(timeSource TimeSource) Option {
	return func(g *Yts3) { g.timeSource = timeSource }
}

func WithTimeSkewLimit(skew time.Duration) Option {
	return func(g *Yts3) { g.timeSkew = skew }
}

func WithMetadataSizeLimit(size int) Option {
	return func(g *Yts3) { g.metadataSizeLimit = size }
}

func WithIntegrityCheck(check bool) Option {
	return func(g *Yts3) { g.integrityCheck = check }
}

func WithLogger(logger Logger) Option {
	return func(g *Yts3) { g.log = logger }
}

func WithGlobalLog() Option {
	return WithLogger(GlobalLog())
}
func WithRequestID(id uint64) Option {
	return func(g *Yts3) { g.requestID.Set(int64(id)) }
}

func WithHostBucket(enabled bool) Option {
	return func(g *Yts3) { g.hostBucket = enabled }
}

func WithoutVersioning() Option {
	return func(g *Yts3) { g.versioned = nil }
}

func WithUnimplementedPageError() Option {
	return func(g *Yts3) { g.failOnUnimplementedPage = true }
}
