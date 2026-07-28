// Package db embeds the SQL migration files so the migrate command can apply
// them without a filesystem dependency.
package db

import "embed"

//go:embed migrations/*.sql
var Migrations embed.FS
