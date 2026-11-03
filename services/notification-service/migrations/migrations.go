// Package migrations embeds the notification-service schema.
package migrations

import "embed"

//go:embed *.sql
var FS embed.FS
