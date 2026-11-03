// Package migrations embeds the order-service schema.
package migrations

import "embed"

//go:embed *.sql
var FS embed.FS
