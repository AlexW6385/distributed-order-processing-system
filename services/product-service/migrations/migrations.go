// Package migrations embeds the product-service schema.
package migrations

import "embed"

//go:embed *.sql
var FS embed.FS
