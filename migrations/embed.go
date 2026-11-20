package migrations

import "embed"

// FS contains all goose migration files. Keeping the embed next to the SQL
// files means the CLI and tests can migrate a fresh database without relying on
// the working directory.
//
//go:embed *.sql
var FS embed.FS
