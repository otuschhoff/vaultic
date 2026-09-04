package feature

// Flag is named such that checking for a feature uses `feature.Flag.Enabled(feature.ExampleFeature)`.
var Flag = New()

// flag names are written in kebab-case
const (
	BackendErrorRedesign    FlagName = "backend-error-redesign"
	DeprecateLegacyIndex    FlagName = "deprecate-legacy-index"
	DeprecateS3LegacyLayout FlagName = "deprecate-s3-legacy-layout"
	DeviceIDForHardlinks    FlagName = "device-id-for-hardlinks"
	ExplicitS3AnonymousAuth FlagName = "explicit-s3-anonymous-auth"
	SafeForgetKeepTags      FlagName = "safe-forget-keep-tags"
	S3Restore               FlagName = "s3-restore"
	SlateDBAuthoritative    FlagName = "slatedb-authoritative"
	WarmupCommand           FlagName = "warmup-command"
	// LockFree makes read-only commands operate without creating lock files.
	// Append commands retain a non-exclusive lock until prune can safely
	// revalidate concurrent writes. Exclusive commands always lock.
	LockFree FlagName = "lock-free"
	// TwoPhasePrune enables the --keep-delete / --instant-delete prune flags.
	// With --keep-delete, prune performs only the repack+index phase and defers
	// deletion of superseded files to a later run, shortening the exclusive
	// window. Alpha while the two-phase path matures.
	TwoPhasePrune FlagName = "two-phase-prune"
)

func init() {
	Flag.SetFlags(map[FlagName]FlagDesc{
		BackendErrorRedesign: {
			Type:        Beta,
			Description: "enforce timeouts for stuck HTTP requests and use new backend error handling design.",
		},
		DeprecateLegacyIndex: {
			Type:        Stable,
			Description: "disable support for index format used by vaultic 0.1.0. Use `vaultic repair index` to update the index if necessary.",
		},
		DeprecateS3LegacyLayout: {
			Type:        Stable,
			Description: "disable support for S3 legacy layout used up to vaultic 0.7.0. Use vaultic 0.17.3 to migrate if necessary.",
		},
		DeviceIDForHardlinks: {
			Type: Alpha,
			Description: ("store deviceID only for hardlinks to reduce metadata changes for example " +
				"when using btrfs subvolumes. Will be removed in a future vaultic version " +
				"after repository format 3 is available"),
		},
		ExplicitS3AnonymousAuth: {
			Type:        Stable,
			Description: "forbid anonymous S3 authentication unless `-o s3.unsafe-anonymous-auth=true` is set",
		},
		SafeForgetKeepTags: {
			Type:        Stable,
			Description: "prevent deleting all snapshots if the tag passed to `forget --keep-tags tagname` does not exist",
		},
		SlateDBAuthoritative: {
			Type:        Alpha,
			Description: "allow repositories with a validated SlateDB manifest to use the daemon-backed authoritative metadata engine",
		},
		S3Restore: {
			Type:        Alpha,
			Description: "restore S3 objects from cold storage classes when `-o s3.enable-restore=true` is set",
		}, WarmupCommand: {Type: Beta, Description: "run the --warm-up-command to warm up cold storage before reading packs"},
		LockFree: {
			Type: Alpha,
			Description: ("let read-only commands (restore, snapshots, ls, find, ...) run without lock " +
				"files; append operations retain non-exclusive locks until concurrent prune " +
				"revalidation is available. Opt-in (VAULTIC_FEATURES=lock-free=true): " +
				"lock-free reorders backend list operations, which is unsafe on " +
				"eventually-consistent backends unless clients coordinate, so it is disabled " +
				"by default."),
		},
		TwoPhasePrune: {
			Type: Alpha,
			Description: ("enable two-phase prune via --keep-delete/--instant-delete; defer deletion " +
				"of superseded packs/indexes to a later prune run to shorten the exclusive " +
				"window"),
		},
	})
}
