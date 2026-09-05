package daemon

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/otuschhoff/vaultic/internal/index/schema"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type GDPRForgetRequest struct {
	UID        uint32
	ExecutedAt int64
	RunID      schema.ID
	SigningKey ed25519.PrivateKey
}

type gdprRevision struct {
	key    []byte
	parsed schema.ParsedKey
	record schema.InodeRevision
	value  []byte
}

type gdprDirectory struct {
	key    []byte
	parsed schema.ParsedKey
	record schema.DirectoryRevision
	value  []byte
}

type gdprInventory struct {
	revisions         []gdprRevision
	manifests         map[schema.ID][]gdprManifestSegment
	orphanedManifests map[schema.ID]struct{}
}

type gdprForgetPlan struct {
	certificate     schema.DeletionCertificateRecord
	puts            []Mutation
	deletes         [][]byte
	purgedHashes    []schema.ID
	remaining       []gdprRevision
	affectedBlobs   map[schema.ID]struct{}
	affectedInodes  map[[2]uint64]struct{}
	targetKeys      map[string]struct{}
	targetRevisions map[[3]uint64]struct{}
	targetPaths     map[string]struct{}
}

func (store *SchemaStore) ExecuteGDPRForget(
	ctx context.Context,
	request GDPRForgetRequest,
) (schema.DeletionCertificateRecord, error) {
	if request.ExecutedAt <= 0 || request.RunID == (schema.ID{}) || len(request.SigningKey) != ed25519.PrivateKeySize {
		return schema.DeletionCertificateRecord{}, fmt.Errorf("invalid GDPR forget request")
	}
	for attempt := 0; attempt < revisionAllocationAttempts; attempt++ {
		certificate, err := store.executeGDPRForget(ctx, request)
		if status.Code(err) != codes.Aborted {
			return certificate, err
		}
	}
	return schema.DeletionCertificateRecord{}, fmt.Errorf("GDPR forget transaction exceeded conflict retry limit")
}

func (store *SchemaStore) executeGDPRForget(
	ctx context.Context,
	request GDPRForgetRequest,
) (schema.DeletionCertificateRecord, error) {
	transaction, err := store.client.Begin(ctx)
	if err != nil {
		return schema.DeletionCertificateRecord{}, err
	}
	fail := func(err error) (schema.DeletionCertificateRecord, error) {
		rollbackTransaction(ctx, transaction)
		return schema.DeletionCertificateRecord{}, err
	}
	certificateKey := schema.DeletionCertificateKey(request.UID, uint64(request.ExecutedAt), request.RunID)
	replayed, err := findGDPRReplay(ctx, transaction, request, certificateKey)
	if err != nil {
		return fail(err)
	}
	if replayed != nil {
		if err := transaction.Rollback(ctx); err != nil {
			return schema.DeletionCertificateRecord{}, fmt.Errorf("close replayed GDPR transaction: %w", err)
		}
		return *replayed, nil
	}

	plan, err := store.planGDPRForget(ctx, transaction, request, certificateKey)
	if err != nil {
		return fail(err)
	}
	if err := writeTransactionBatches(ctx, transaction, store.client.Limits(), plan.puts, plan.deletes); err != nil {
		return fail(err)
	}
	if err := transaction.Commit(ctx); err != nil {
		return schema.DeletionCertificateRecord{}, err
	}
	return plan.certificate, nil
}

func (store *SchemaStore) planGDPRForget(
	ctx context.Context,
	transaction *Transaction,
	request GDPRForgetRequest,
	certificateKey []byte,
) (gdprForgetPlan, error) {
	inventory, err := buildGDPRInventory(ctx, transaction, request.UID)
	if err != nil {
		return gdprForgetPlan{}, err
	}
	plan, err := planGDPRRevisionRedactions(inventory, request.UID)
	if err != nil {
		return gdprForgetPlan{}, err
	}
	directories, err := scanGDPRDirectories(ctx, transaction)
	if err != nil {
		return gdprForgetPlan{}, err
	}
	directoryPlan, err := planGDPRDirectoryRedactions(directories, request.UID, plan)
	if err != nil {
		return gdprForgetPlan{}, err
	}
	plan.puts = append(plan.puts, directoryPlan.puts...)
	plan.deletes = append(plan.deletes, directoryPlan.deletes...)
	plan.purgedHashes = append(plan.purgedHashes, directoryPlan.purgedHashes...)
	pathDeletes, err := planGDPRPathDeletes(ctx, transaction, plan.targetPaths, plan.targetRevisions)
	if err != nil {
		return gdprForgetPlan{}, err
	}
	plan.deletes = append(plan.deletes, pathDeletes...)
	cleanupDeletes, err := planGDPRGlobalCleanup(ctx, transaction, inventory, plan.affectedBlobs)
	if err != nil {
		return gdprForgetPlan{}, err
	}
	plan.deletes = append(plan.deletes, cleanupDeletes...)
	counts, referencePlan, err := planGDPRReferenceRebuild(
		inventory,
		plan.remaining,
		plan.affectedBlobs,
		plan.affectedInodes,
		request.ExecutedAt,
	)
	if err != nil {
		return gdprForgetPlan{}, err
	}
	plan.puts = append(plan.puts, referencePlan.puts...)
	plan.deletes = append(plan.deletes, referencePlan.deletes...)
	var finalPuts []Mutation
	plan.certificate, finalPuts, err = planGDPRCertificate(
		ctx,
		transaction,
		request,
		certificateKey,
		plan.purgedHashes,
		plan.affectedBlobs,
		counts,
	)
	if err != nil {
		return gdprForgetPlan{}, err
	}
	plan.puts = append(plan.puts, finalPuts...)
	plan.deletes = uniqueGDPRKeys(plan.deletes)
	return plan, nil
}

func findGDPRReplay(
	ctx context.Context,
	transaction *Transaction,
	request GDPRForgetRequest,
	certificateKey []byte,
) (*schema.DeletionCertificateRecord, error) {
	if value, found, err := transaction.Get(ctx, certificateKey); err != nil {
		return nil, err
	} else if found {
		certificate, err := schema.UnmarshalDeletionCertificateRecord(value)
		return &certificate, err
	}
	var replayed *schema.DeletionCertificateRecord
	err := scanGDPRTransaction(
		ctx,
		transaction,
		schema.DeletionCertificatePrefix(request.UID),
		func(kv KeyValue) error {
			certificate, err := schema.UnmarshalDeletionCertificateRecord(kv.Value)
			if err != nil {
				return err
			}
			if certificate.RunID == request.RunID {
				copy := certificate
				replayed = &copy
			}
			return nil
		},
	)
	return replayed, err
}

func buildGDPRInventory(ctx context.Context, transaction *Transaction, uid uint32) (gdprInventory, error) {
	revisions, err := scanGDPRRevisions(ctx, transaction)
	if err != nil {
		return gdprInventory{}, err
	}
	manifests, err := scanGDPRManifests(ctx, transaction)
	if err != nil {
		return gdprInventory{}, err
	}
	manifestUses := make(map[schema.ID]uint64)
	targetUses := make(map[schema.ID]uint64)
	for _, revision := range revisions {
		if revision.record.ContentMode != schema.ContentManifestRef {
			continue
		}
		manifestUses[revision.record.ContentManifestID]++
		if revision.record.Known&schema.KnownUID != 0 && revision.record.UID == uid {
			targetUses[revision.record.ContentManifestID]++
		}
	}
	orphaned := make(map[schema.ID]struct{})
	for id, count := range targetUses {
		if count == manifestUses[id] {
			orphaned[id] = struct{}{}
		}
	}
	return gdprInventory{revisions: revisions, manifests: manifests, orphanedManifests: orphaned}, nil
}

func planGDPRRevisionRedactions(inventory gdprInventory, uid uint32) (gdprForgetPlan, error) {
	plan := gdprForgetPlan{
		remaining: make([]gdprRevision, 0, len(inventory.revisions)), affectedBlobs: make(map[schema.ID]struct{}),
		affectedInodes: make(map[[2]uint64]struct{}), targetKeys: make(map[string]struct{}),
		targetRevisions: make(map[[3]uint64]struct{}), targetPaths: make(map[string]struct{}),
	}
	for _, revision := range inventory.revisions {
		if revision.record.Known&schema.KnownUID == 0 || revision.record.UID != uid {
			plan.remaining = append(plan.remaining, revision)
			continue
		}
		for _, id := range gdprRevisionContent(revision.record, inventory.manifests) {
			plan.affectedBlobs[id] = struct{}{}
		}
		plan.affectedInodes[[2]uint64{uint64(revision.parsed.FSID), revision.parsed.Inode}] = struct{}{}
		plan.targetKeys[string(revision.key)] = struct{}{}
		plan.targetRevisions[[3]uint64{uint64(revision.parsed.FSID), revision.parsed.Inode, revision.parsed.Revision}] = struct{}{}
		if revision.record.SourcePath != "" {
			plan.targetPaths[strings.TrimPrefix(revision.record.SourcePath, "/")] = struct{}{}
		}
		plan.purgedHashes = append(plan.purgedHashes, gdprRecordHash(revision.key, revision.value))
		redacted := revision.record
		redacted.Size, redacted.UID, redacted.SourcePath = 0, 0, ""
		redacted.Known &^= schema.KnownSize | schema.KnownUID | schema.KnownPath
		redacted.ContentMode, redacted.ContentCount = 0, 0
		redacted.ContentIDs, redacted.ContentManifestID = nil, schema.ID{}
		redacted.FileContentHash, redacted.HashKnown = schema.ID{}, false
		value, err := redacted.MarshalBinary()
		if err != nil {
			return gdprForgetPlan{}, err
		}
		plan.puts = append(plan.puts, Mutation{Key: revision.key, Value: value})
		plan.deletes = append(
			plan.deletes,
			schema.HardlinkRefsKey(revision.parsed.FSID, revision.parsed.Inode, revision.parsed.Revision),
		)
	}
	return plan, nil
}

func gdprRecordHash(key, value []byte) schema.ID {
	input := append(append([]byte(nil), key...), value...)
	return schema.ID(sha256.Sum256(input))
}

func planGDPRDirectoryRedactions(
	directories []gdprDirectory,
	uid uint32,
	targets gdprForgetPlan,
) (gdprForgetPlan, error) {
	for _, directory := range directories {
		if directory.record.Known&schema.KnownUID == 0 || directory.record.UID != uid {
			continue
		}
		targets.targetKeys[string(directory.key)] = struct{}{}
		targets.targetRevisions[[3]uint64{uint64(directory.parsed.FSID), directory.parsed.Inode, directory.parsed.Revision}] = struct{}{}
		if directory.record.SourcePath != "" {
			targets.targetPaths[strings.TrimPrefix(directory.record.SourcePath, "/")] = struct{}{}
		}
	}
	result := gdprForgetPlan{}
	for _, directory := range directories {
		changed := pruneGDPRDirectoryChildren(&directory.record, targets.targetKeys)
		if directory.record.Known&schema.KnownUID != 0 && directory.record.UID == uid {
			directory.record.Size, directory.record.UID, directory.record.SourcePath = 0, 0, ""
			directory.record.Known &^= schema.KnownSize | schema.KnownUID | schema.KnownPath
			changed = true
			result.deletes = append(
				result.deletes,
				schema.HardlinkRefsKey(directory.parsed.FSID, directory.parsed.Inode, directory.parsed.Revision),
			)
		}
		if !changed {
			continue
		}
		value, err := directory.record.MarshalBinary()
		if err != nil {
			return gdprForgetPlan{}, err
		}
		result.purgedHashes = append(result.purgedHashes, gdprRecordHash(directory.key, directory.value))
		result.puts = append(result.puts, Mutation{Key: directory.key, Value: value})
	}
	return result, nil
}

func pruneGDPRDirectoryChildren(record *schema.DirectoryRevision, targets map[string]struct{}) bool {
	changed := false
	children := record.Children[:0]
	for _, child := range record.Children {
		if _, targeted := targets[string(child.MetadataKey)]; targeted {
			changed = true
			continue
		}
		children = append(children, child)
	}
	record.Children = children
	return changed
}

func planGDPRPathDeletes(
	ctx context.Context,
	transaction *Transaction,
	targetPaths map[string]struct{},
	targetRevisions map[[3]uint64]struct{},
) ([][]byte, error) {
	var deletes [][]byte
	err := scanGDPRTransaction(ctx, transaction, []byte("pv:"), func(kv KeyValue) error {
		key, err := schema.ParseKey(kv.Key)
		if err != nil {
			return err
		}
		record, err := schema.UnmarshalPathVersionRecord(kv.Value)
		if err != nil {
			return err
		}
		_, pathTargeted := targetPaths[strings.TrimPrefix(key.Path, "/")]
		if !pathTargeted && record.Path != "" {
			_, pathTargeted = targetPaths[strings.TrimPrefix(record.Path, "/")]
		}
		revisionTargeted := record.State == schema.PathBound
		if revisionTargeted {
			_, revisionTargeted = targetRevisions[[3]uint64{uint64(key.FSID), record.Inode, record.Revision}]
		}
		if pathTargeted || revisionTargeted {
			deletes = append(deletes, kv.Key)
		}
		return nil
	})
	return deletes, err
}

func planGDPRGlobalCleanup(
	ctx context.Context,
	transaction *Transaction,
	inventory gdprInventory,
	affectedBlobs map[schema.ID]struct{},
) ([][]byte, error) {
	var deletes [][]byte
	for manifestID := range inventory.orphanedManifests {
		for _, item := range inventory.manifests[manifestID] {
			deletes = append(deletes, item.key)
			for _, blob := range item.record.ContentIDs {
				affectedBlobs[blob] = struct{}{}
				deletes = append(deletes, schema.ReverseManifestKey(blob, manifestID))
			}
		}
	}
	for _, prefix := range gdprAnalyticsPrefixes() {
		err := scanGDPRTransaction(ctx, transaction, prefix, func(kv KeyValue) error {
			deletes = append(deletes, kv.Key)
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	return deletes, nil
}

func planGDPRReferenceRebuild(
	inventory gdprInventory,
	remaining []gdprRevision,
	affectedBlobs map[schema.ID]struct{},
	affectedInodes map[[2]uint64]struct{},
	executedAt int64,
) (map[schema.ID]schema.ReferenceCountRecord, gdprForgetPlan, error) {
	manifestEdges := make([]schema.ManifestEdge, 0)
	for manifestID, segments := range inventory.manifests {
		if _, orphaned := inventory.orphanedManifests[manifestID]; orphaned {
			continue
		}
		for _, segment := range segments {
			for _, blob := range segment.record.ContentIDs {
				manifestEdges = append(manifestEdges, schema.ManifestEdge{Blob: blob, Manifest: manifestID})
			}
		}
	}
	inodeEdges, remainingBlobs := gdprRemainingInodeEdges(remaining, inventory.manifests)
	counts := schema.RebuildReferenceCounts(manifestEdges, inodeEdges, uint64(executedAt))
	plan := gdprForgetPlan{}
	for blob := range affectedBlobs {
		if count, found := counts[blob]; found {
			value, err := count.MarshalBinary()
			if err != nil {
				return nil, gdprForgetPlan{}, err
			}
			plan.puts = append(plan.puts, Mutation{Key: schema.ReferenceCountKey(blob), Value: value})
		} else {
			plan.deletes = append(plan.deletes, schema.ReferenceCountKey(blob))
		}
		for inode := range affectedInodes {
			fsid, number := uint32(inode[0]), inode[1]
			if _, found := remainingBlobs[gdprInodeBlobKey(blob, fsid, number)]; !found {
				plan.deletes = append(plan.deletes, schema.ReverseInodeKey(blob, fsid, number))
			}
		}
	}
	return counts, plan, nil
}

func gdprRemainingInodeEdges(
	revisions []gdprRevision,
	manifests map[schema.ID][]gdprManifestSegment,
) ([]schema.InodeEdge, map[[52]byte]struct{}) {
	edges := make([]schema.InodeEdge, 0)
	blobs := make(map[[52]byte]struct{})
	for _, revision := range revisions {
		for _, blob := range gdprRevisionContent(revision.record, manifests) {
			edges = append(
				edges,
				schema.InodeEdge{
					Blob:     blob,
					FSID:     revision.parsed.FSID,
					Inode:    revision.parsed.Inode,
					Revision: revision.parsed.Revision,
				},
			)
			blobs[gdprInodeBlobKey(blob, revision.parsed.FSID, revision.parsed.Inode)] = struct{}{}
		}
	}
	return edges, blobs
}

func planGDPRCertificate(
	ctx context.Context,
	transaction *Transaction,
	request GDPRForgetRequest,
	certificateKey []byte,
	purgedHashes []schema.ID,
	affectedBlobs map[schema.ID]struct{},
	counts map[schema.ID]schema.ReferenceCountRecord,
) (schema.DeletionCertificateRecord, []Mutation, error) {
	pending, puts, events, err := planGDPRPackDeletion(ctx, transaction, affectedBlobs, counts, request)
	if err != nil {
		return schema.DeletionCertificateRecord{}, nil, err
	}
	history, err := packHistoryMutations(ctx, transaction, events)
	if err != nil {
		return schema.DeletionCertificateRecord{}, nil, err
	}
	puts = append(puts, history...)
	sort.Slice(purgedHashes, func(i, j int) bool { return string(purgedHashes[i][:]) < string(purgedHashes[j][:]) })
	certificate := schema.DeletionCertificateRecord{
		UID: request.UID, ExecutedAt: request.ExecutedAt, RunID: request.RunID,
		PurgedReferenceHashes: uniqueGDPRIDs(purgedHashes), PendingDeletion: pending,
		SigningAlgorithm: "Ed25519", PublicKey: request.SigningKey.Public().(ed25519.PublicKey),
	}
	signingBytes, err := certificate.SigningBytes()
	if err != nil {
		return schema.DeletionCertificateRecord{}, nil, err
	}
	certificate.Signature = ed25519.Sign(request.SigningKey, signingBytes)
	value, err := certificate.MarshalBinary()
	if err != nil {
		return schema.DeletionCertificateRecord{}, nil, err
	}
	puts = append(puts, Mutation{Key: certificateKey, Value: value})
	return certificate, puts, nil
}

type gdprManifestSegment struct {
	key    []byte
	record schema.ContentManifest
}

func scanGDPRRevisions(ctx context.Context, transaction *Transaction) ([]gdprRevision, error) {
	result := make([]gdprRevision, 0)
	err := scanGDPRTransaction(ctx, transaction, []byte("iv:"), func(kv KeyValue) error {
		parsed, err := schema.ParseKey(kv.Key)
		if err != nil {
			return err
		}
		record, err := schema.UnmarshalInodeRevision(kv.Value)
		if err != nil {
			return err
		}
		result = append(result, gdprRevision{key: kv.Key, parsed: parsed, record: record, value: kv.Value})
		return nil
	})
	return result, err
}

func scanGDPRDirectories(ctx context.Context, transaction *Transaction) ([]gdprDirectory, error) {
	result := make([]gdprDirectory, 0)
	err := scanGDPRTransaction(ctx, transaction, []byte("dv:"), func(kv KeyValue) error {
		parsed, err := schema.ParseKey(kv.Key)
		if err != nil {
			return err
		}
		record, err := schema.UnmarshalDirectoryRevision(kv.Value)
		if err != nil {
			return err
		}
		result = append(result, gdprDirectory{key: kv.Key, parsed: parsed, record: record, value: kv.Value})
		return nil
	})
	return result, err
}

func scanGDPRManifests(ctx context.Context, transaction *Transaction) (map[schema.ID][]gdprManifestSegment, error) {
	result := make(map[schema.ID][]gdprManifestSegment)
	err := scanGDPRTransaction(ctx, transaction, []byte("cm:"), func(kv KeyValue) error {
		parsed, err := schema.ParseKey(kv.Key)
		if err != nil {
			return err
		}
		record, err := schema.UnmarshalContentManifest(kv.Value)
		if err != nil {
			return err
		}
		result[parsed.ID] = append(result[parsed.ID], gdprManifestSegment{key: kv.Key, record: record})
		return nil
	})
	return result, err
}

func scanGDPRTransaction(
	ctx context.Context,
	transaction *Transaction,
	prefix []byte,
	visit func(KeyValue) error,
) error {
	var cursor []byte
	for {
		items, done, err := transaction.ScanPage(ctx, prefix, cursor, 1_000)
		if err != nil {
			return err
		}
		for _, item := range items {
			if err := visit(item); err != nil {
				return err
			}
			cursor = item.Key
		}
		if done {
			return nil
		}
	}
}

func gdprRevisionContent(record schema.InodeRevision, manifests map[schema.ID][]gdprManifestSegment) []schema.ID {
	if record.ContentMode == schema.ContentInline {
		return record.ContentIDs
	}
	if record.ContentMode != schema.ContentManifestRef {
		return nil
	}
	var result []schema.ID
	for _, segment := range manifests[record.ContentManifestID] {
		result = append(result, segment.record.ContentIDs...)
	}
	return result
}

func gdprInodeBlobKey(blob schema.ID, fsid uint32, inode uint64) [52]byte {
	var key [52]byte
	copy(key[:32], blob[:])
	key[32], key[33], key[34], key[35] = byte(fsid>>24), byte(fsid>>16), byte(fsid>>8), byte(fsid)
	for index := 0; index < 8; index++ {
		key[36+index] = byte(inode >> (56 - 8*index))
	}
	return key
}

func uniqueGDPRIDs(ids []schema.ID) []schema.ID {
	result := ids[:0]
	for _, id := range ids {
		if len(result) == 0 || result[len(result)-1] != id {
			result = append(result, id)
		}
	}
	return result
}

func uniqueGDPRKeys(keys [][]byte) [][]byte {
	sort.Slice(keys, func(i, j int) bool { return string(keys[i]) < string(keys[j]) })
	result := keys[:0]
	for _, key := range keys {
		if len(result) == 0 || string(result[len(result)-1]) != string(key) {
			result = append(result, key)
		}
	}
	return result
}

func gdprAnalyticsPrefixes() [][]byte {
	return [][]byte{
		schema.AnalyticsFactPrefix(), schema.AnalyticsFactSegmentPrefix(), schema.AnalyticsSegmentMetadataPrefix(),
		[]byte(
			"ai:",
		), []byte("ar:"), []byte("ad:"), schema.AnalyticsManifestPrefix(), schema.AnalyticsWatermarkPrefix(),
		schema.AnalyticsCachePrefix(), []byte("av1:"), []byte("g:time:"), []byte("g:path:"), []byte("u:summary:"),
		[]byte("g:summary:"), []byte("u:statsv1:"), []byte("g:statsv1:"), []byte("u:churn:"), []byte("u:inodes:"),
		[]byte("u:blobs:"), []byte("u:blobv1:"), schema.AnalyticsBuildCheckpointKey(), schema.AnalyticsMetadataKey(),
	}
}

func planGDPRPackDeletion(
	ctx context.Context,
	transaction *Transaction,
	affected map[schema.ID]struct{},
	counts map[schema.ID]schema.ReferenceCountRecord,
	request GDPRForgetRequest,
) ([]schema.DeletionSchedule, []Mutation, []PackEvent, error) {
	eligible, packRecords, err := discoverGDPRDeletionPacks(ctx, transaction, affected, counts)
	if err != nil {
		return nil, nil, nil, err
	}
	var schedules []schema.DeletionSchedule
	var puts []Mutation
	scheduledPacks := make(map[schema.ID]struct{})
	err = scanGDPRTransaction(ctx, transaction, []byte("pl:"), func(kv KeyValue) error {
		parsed, err := schema.ParseKey(kv.Key)
		if err != nil {
			return err
		}
		if _, found := eligible[parsed.ID]; !found {
			return nil
		}
		placement, err := schema.UnmarshalPlacementRecord(kv.Value)
		if err != nil {
			return err
		}
		deleteAfter := time.Unix(request.ExecutedAt, 0).UnixNano()
		if placement.MinRetentionUntil > deleteAfter {
			deleteAfter = placement.MinRetentionUntil
		}
		if placement.State == schema.PlacementEvicting && placement.DeleteAfter != 0 {
			deleteAfter = placement.DeleteAfter
		} else if placement.State == schema.PlacementLive || placement.State == schema.PlacementPending {
			placement.State, placement.DeleteAfter = schema.PlacementEvicting, deleteAfter
			value, err := placement.MarshalBinary()
			if err != nil {
				return err
			}
			puts = append(puts, Mutation{Key: kv.Key, Value: value})
			backendValue, err := (schema.BackendPackRecord{State: placement.State, Bytes: placement.Bytes, PlacedAt: placement.PlacedAt}).MarshalBinary()
			if err != nil {
				return err
			}
			puts = append(puts, Mutation{Key: schema.BackendPackKey(parsed.Backend, parsed.ID), Value: backendValue})
		} else {
			return nil
		}
		schedule := schema.DeletionSchedule{PackID: parsed.ID, Backend: parsed.Backend, DeleteAfter: deleteAfter}
		value,
			err := (schema.PlacementDeleteRecord{Backend: parsed.Backend,
			PhysicalSize: placement.Bytes,
			Reason:       "gdpr-forget",
			RunID:        request.RunID}).MarshalBinary()
		if err != nil {
			return err
		}
		schedules = append(schedules, schedule)
		puts = append(
			puts,
			Mutation{Key: schema.PlacementDeleteQueueKey(deleteAfter, parsed.ID, parsed.Backend), Value: value},
		)
		scheduledPacks[parsed.ID] = struct{}{}
		return nil
	})
	if err != nil {
		return nil, nil, nil, err
	}
	return finalizeGDPRPackDeletion(schedules, puts, scheduledPacks, packRecords, request)
}

func discoverGDPRDeletionPacks(
	ctx context.Context,
	transaction *Transaction,
	affected map[schema.ID]struct{},
	counts map[schema.ID]schema.ReferenceCountRecord,
) (map[schema.ID]struct{}, map[schema.ID]schema.PackRecord, error) {
	packBlobs, touchedPacks, err := scanGDPRPackBlobs(ctx, transaction, affected)
	if err != nil {
		return nil, nil, err
	}
	eligible := make(map[schema.ID]struct{})
	packRecords := make(map[schema.ID]schema.PackRecord)
	for pack, blobs := range packBlobs {
		if _, touched := touchedPacks[pack]; !touched || gdprPackHasReferences(blobs, counts) {
			continue
		}
		value, found, err := transaction.Get(ctx, schema.PackKey(pack))
		if err != nil {
			return nil, nil, err
		}
		if !found {
			continue
		}
		record, err := schema.UnmarshalPackRecord(value)
		if err != nil {
			return nil, nil, err
		}
		if record.Lifecycle == schema.PackPublished || record.Lifecycle == schema.PackDeletePending {
			eligible[pack], packRecords[pack] = struct{}{}, record
		}
	}
	return eligible, packRecords, nil
}

func scanGDPRPackBlobs(
	ctx context.Context,
	transaction *Transaction,
	affected map[schema.ID]struct{},
) (map[schema.ID][]schema.ID, map[schema.ID]struct{}, error) {
	packBlobs := make(map[schema.ID][]schema.ID)
	touchedPacks := make(map[schema.ID]struct{})
	if err := scanGDPRTransaction(ctx, transaction, []byte("b:"), func(kv KeyValue) error {
		parsed, err := schema.ParseKey(kv.Key)
		if err != nil {
			return err
		}
		record, err := schema.UnmarshalBlobRecord(kv.Value)
		if err != nil {
			return err
		}
		for _, location := range record.Locations {
			packBlobs[location.PackID] = append(packBlobs[location.PackID], parsed.ID)
			if _, relevant := affected[parsed.ID]; relevant {
				touchedPacks[location.PackID] = struct{}{}
			}
		}
		return nil
	}); err != nil {
		return nil, nil, err
	}
	return packBlobs, touchedPacks, nil
}

func gdprPackHasReferences(blobs []schema.ID, counts map[schema.ID]schema.ReferenceCountRecord) bool {
	for _, blob := range blobs {
		if counts[blob].TotalReferences != 0 {
			return true
		}
	}
	return false
}

func finalizeGDPRPackDeletion(
	schedules []schema.DeletionSchedule,
	puts []Mutation,
	scheduledPacks map[schema.ID]struct{},
	packRecords map[schema.ID]schema.PackRecord,
	request GDPRForgetRequest,
) ([]schema.DeletionSchedule, []Mutation, []PackEvent, error) {
	var events []PackEvent
	for pack := range scheduledPacks {
		record := packRecords[pack]
		if record.Lifecycle == schema.PackDeletePending {
			continue
		}
		record.Lifecycle = schema.PackDeletePending
		value, err := record.MarshalBinary()
		if err != nil {
			return nil, nil, nil, err
		}
		puts = append(puts, Mutation{Key: schema.PackKey(pack), Value: value})
		events = append(
			events,
			PackEvent{
				PackID: pack,
				Record: schema.PackHistoryEvent{
					Type:         schema.EventDeletePending,
					PackType:     record.Type,
					PhysicalSize: record.PhysicalSize,
					PayloadSize:  record.PayloadSize,
					RunID:        request.RunID,
				},
			},
		)
	}
	sort.Slice(schedules, func(i, j int) bool {
		if schedules[i].PackID != schedules[j].PackID {
			return string(schedules[i].PackID[:]) < string(schedules[j].PackID[:])
		}
		if schedules[i].Backend != schedules[j].Backend {
			return schedules[i].Backend < schedules[j].Backend
		}
		return schedules[i].DeleteAfter < schedules[j].DeleteAfter
	})
	return schedules, puts, events, nil
}
