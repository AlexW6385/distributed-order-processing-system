// Package requestid carries a per-request correlation ID through contexts so
// one client request can be followed across the gateway and every service it
// touches.
package requestid

import "context"

type key struct{}

func With(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, key{}, id)
}

func From(ctx context.Context) string {
	id, _ := ctx.Value(key{}).(string)
	return id
}
