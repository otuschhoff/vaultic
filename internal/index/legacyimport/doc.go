// Package legacyimport imports recoverable legacy JSON index and bounded
// snapshot-tree metadata into the daemon-backed schema without making it
// authoritative. Import is resumable and records unresolved facts as durable
// crawl debt for later reconciliation.
package legacyimport
