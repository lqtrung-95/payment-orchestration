// Package migrations embeds the SQL migration files into the binary.
//
// Embedding rather than reading from disk means the migration runner has no
// working-directory dependency: the same binary migrates from a container, from
// CI, and from a developer machine without path juggling. It also guarantees
// the migrations applied are exactly the ones the binary was built from.
package migrations

import "embed"

//go:embed *.sql
var FS embed.FS
