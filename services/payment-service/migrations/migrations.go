// Package migrations embeds the payment-service schema.
package migrations

import "embed"

//go:embed *.sql
var FS embed.FS
