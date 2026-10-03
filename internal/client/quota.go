package client

import "context"

type quotaKey struct{}

// WithQuota limits the namespace (first path segment) that Put, BeginResumable
// and Undelete write to: the metadata log refuses the request if the
// namespace's live bytes plus pending uploads plus this one would pass limit.
// Zero or less is unlimited. Set by a gateway that authenticates its callers
// (ADR-0025); the metadata server trusts it as it trusts any client.
func WithQuota(ctx context.Context, limit int64) context.Context {
	return context.WithValue(ctx, quotaKey{}, limit)
}

func quotaOf(ctx context.Context) int64 {
	n, _ := ctx.Value(quotaKey{}).(int64)
	return n
}
