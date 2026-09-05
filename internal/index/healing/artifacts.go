package healing

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/otuschhoff/vaultic/internal/index/daemon"
)

const (
	Format       = 1
	planPrefix   = "plan-"
	reportPrefix = "report-"
)

func DeriveKey(repositoryKey []byte, repositoryID string) ([]byte, error) {
	if len(repositoryKey) < 32 || repositoryID == "" {
		return nil, fmt.Errorf("invalid healing key derivation input")
	}
	mac := hmac.New(sha256.New, repositoryKey)
	_, _ = mac.Write([]byte("vaultic-metadata-healing-v1\x00")) // hash.Hash writes are specified to return a nil error.
	_, _ = mac.Write([]byte(repositoryID))                      // hash.Hash writes are specified to return a nil error.
	return mac.Sum(nil), nil
}

type Source struct {
	Kind          string `json:"kind"`
	ID            string `json:"id"`
	Authority     uint32 `json:"authority"`
	Authenticated bool   `json:"authenticated"`
	State         string `json:"state,omitempty"`
	Objects       uint64 `json:"objects,omitempty"`
	Bytes         uint64 `json:"bytes,omitempty"`
	Omission      string `json:"omission,omitempty"`
}

type Inventory struct {
	TopologyGeneration uint64   `json:"topology_generation"`
	TopologySHA256     string   `json:"topology_sha256"`
	CapsuleGeneration  uint64   `json:"capsule_generation"`
	PackBackends       []string `json:"pack_backends"`
	LegacyIndexes      uint64   `json:"legacy_indexes"`
	LegacySnapshots    uint64   `json:"legacy_snapshots"`
	Sources            []Source `json:"sources"`
	RequiredCrawlDebt  []string `json:"required_crawl_debt"`
	EstimatedObjects   uint64   `json:"estimated_objects"`
	EstimatedBytes     uint64   `json:"estimated_bytes"`
}

type Plan struct {
	Format              uint32    `json:"format"`
	ID                  string    `json:"id"`
	RepositoryID        string    `json:"repository_id"`
	AuthorityDecision   uint64    `json:"authority_decision"`
	SuspectGeneration   uint64    `json:"suspect_generation"`
	CandidateGeneration uint64    `json:"candidate_generation"`
	CandidateNamespace  string    `json:"candidate_namespace"`
	TargetDEKPolicy     string    `json:"target_dek_policy"`
	CreatedAt           time.Time `json:"created_at"`
	Inventory           Inventory `json:"inventory"`
	Signature           string    `json:"signature"`
}

type Checkpoint struct {
	Format            uint32    `json:"format"`
	PlanID            string    `json:"plan_id"`
	State             string    `json:"state"`
	SourcesCompleted  []string  `json:"sources_completed"`
	JournalsReplayed  []string  `json:"journals_replayed"`
	ObjectsWritten    uint64    `json:"objects_written"`
	BytesWritten      uint64    `json:"bytes_written"`
	UpdatedAt         time.Time `json:"updated_at"`
	CandidateReadOnly bool      `json:"candidate_read_only"`
	Signature         string    `json:"signature"`
}

type Gates struct {
	Identity            bool `json:"identity"`
	AntiRollback        bool `json:"anti_rollback"`
	StructuralAEAD      bool `json:"structural_aead"`
	PacksAndBlobOffsets bool `json:"packs_and_blob_offsets"`
	TreesAndSnapshots   bool `json:"trees_and_snapshots"`
	PlacementPolicy     bool `json:"placement_policy"`
	JournalCompletions  bool `json:"journal_completions"`
	LegacyComparison    bool `json:"legacy_comparison"`
	ReadOnlyInspection  bool `json:"read_only_inspection"`
}

type Report struct {
	Format            uint32            `json:"format"`
	ID                string            `json:"id"`
	PlanID            string            `json:"plan_id"`
	RepositoryID      string            `json:"repository_id"`
	Generation        uint64            `json:"generation"`
	CreatedAt         time.Time         `json:"created_at"`
	Gates             Gates             `json:"gates"`
	CriticalConflicts []string          `json:"critical_conflicts"`
	Omissions         []string          `json:"omissions"`
	CrawlDebt         []string          `json:"crawl_debt"`
	JournalOutcomes   []Source          `json:"journal_outcomes"`
	ObjectCounts      map[string]uint64 `json:"object_counts"`
	Signature         string            `json:"signature"`
}

func NewPlan(repositoryID, namespace, dekPolicy string, authority daemon.GenerationStatus, inventory Inventory, key []byte, now time.Time) (Plan, error) {
	plan := Plan{
		Format:              Format,
		RepositoryID:        repositoryID,
		AuthorityDecision:   authority.Decision,
		SuspectGeneration:   authority.ActiveGeneration,
		CandidateGeneration: authority.ActiveGeneration + 1,
		CandidateNamespace:  namespace,
		TargetDEKPolicy:     dekPolicy,
		CreatedAt:           now.UTC(),
		Inventory:           inventory,
	}
	if authority.ActiveGeneration == ^uint64(0) {
		return Plan{}, fmt.Errorf("metadata generation overflow")
	}
	if err := validatePlanFields(plan); err != nil {
		return Plan{}, err
	}
	plan.ID, plan.Signature = signArtifact(plan, key)
	return plan, nil
}

func VerifyPlan(plan Plan, key []byte) error {
	if err := validatePlanFields(plan); err != nil {
		return err
	}
	id, signature := plan.ID, plan.Signature
	plan.ID, plan.Signature = "", ""
	expectedID, expectedSignature := signArtifact(plan, key)
	if id != expectedID || !hmac.Equal([]byte(signature), []byte(expectedSignature)) {
		return fmt.Errorf("healing plan authentication failed")
	}
	return nil
}

func NewReport(plan Plan, gates Gates, conflicts, omissions []string, outcomes []Source, counts map[string]uint64, key []byte, now time.Time) (Report, error) {
	report := Report{
		Format:            Format,
		PlanID:            plan.ID,
		RepositoryID:      plan.RepositoryID,
		Generation:        plan.CandidateGeneration,
		CreatedAt:         now.UTC(),
		Gates:             gates,
		CriticalConflicts: conflicts,
		Omissions:         omissions,
		CrawlDebt:         append([]string(nil), plan.Inventory.RequiredCrawlDebt...),
		JournalOutcomes:   outcomes,
		ObjectCounts:      counts,
	}
	report.ID, report.Signature = signArtifact(report, key)
	return report, nil
}

func VerifyReport(report Report, plan Plan, key []byte) error {
	if report.Format != Format || report.PlanID != plan.ID || report.RepositoryID != plan.RepositoryID || report.Generation != plan.CandidateGeneration ||
		!report.Clean() {
		return fmt.Errorf("healing report does not authorize this candidate")
	}
	id, signature := report.ID, report.Signature
	report.ID, report.Signature = "", ""
	expectedID, expectedSignature := signArtifact(report, key)
	if id != expectedID || !hmac.Equal([]byte(signature), []byte(expectedSignature)) {
		return fmt.Errorf("healing report authentication failed")
	}
	return nil
}

func (report Report) Clean() bool {
	gates := report.Gates
	return gates.Identity && gates.AntiRollback && gates.StructuralAEAD && gates.PacksAndBlobOffsets && gates.TreesAndSnapshots && gates.PlacementPolicy &&
		gates.JournalCompletions &&
		gates.LegacyComparison &&
		gates.ReadOnlyInspection &&
		len(report.CriticalConflicts) == 0
}

func SignCheckpoint(checkpoint Checkpoint, key []byte) Checkpoint {
	checkpoint.Format = Format
	checkpoint.UpdatedAt = checkpoint.UpdatedAt.UTC()
	checkpoint.Signature = ""
	_, checkpoint.Signature = signArtifact(checkpoint, key)
	return checkpoint
}

func VerifyCheckpoint(checkpoint Checkpoint, planID string, key []byte) error {
	if checkpoint.Format != Format || checkpoint.PlanID != planID || checkpoint.State == "" {
		return fmt.Errorf("invalid healing checkpoint")
	}
	signature := checkpoint.Signature
	checkpoint.Signature = ""
	_, expected := signArtifact(checkpoint, key)
	if !hmac.Equal([]byte(signature), []byte(expected)) {
		return fmt.Errorf("healing checkpoint authentication failed")
	}
	return nil
}

type Store struct{ Directory string }

func (store Store) SavePlan(plan Plan) error {
	return store.saveCreate(planPrefix+plan.ID+".json", plan)
}
func (store Store) SaveReport(report Report) error {
	return store.saveCreate(reportPrefix+report.ID+".json", report)
}
func (store Store) SaveCheckpoint(checkpoint Checkpoint) error {
	return store.saveReplace("checkpoint-"+checkpoint.PlanID+".json", checkpoint)
}

func (store Store) LoadPlan(id string) (Plan, error) {
	var plan Plan
	err := store.load(planPrefix+cleanID(id)+".json", &plan)
	return plan, err
}

func (store Store) LoadReport(id string) (Report, error) {
	var report Report
	err := store.load(reportPrefix+cleanID(id)+".json", &report)
	return report, err
}

func (store Store) LoadCheckpoint(planID string) (Checkpoint, error) {
	var checkpoint Checkpoint
	err := store.load("checkpoint-"+cleanID(planID)+".json", &checkpoint)
	return checkpoint, err
}

func (store Store) saveCreate(name string, value any) error {
	if err := os.MkdirAll(store.Directory, 0o700); err != nil {
		return err
	}
	encoded, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	file, err := os.OpenFile(filepath.Join(store.Directory, name), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	if _, err = file.Write(append(encoded, '\n')); err == nil {
		err = file.Sync()
	}
	closeErr := file.Close()
	return errors.Join(err, closeErr)
}

func (store Store) saveReplace(name string, value any) error {
	if err := os.MkdirAll(store.Directory, 0o700); err != nil {
		return err
	}
	encoded, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	temporary, err := os.CreateTemp(store.Directory, ".checkpoint-")
	if err != nil {
		return err
	}
	temporaryName := temporary.Name()
	defer os.Remove(temporaryName)
	if err = temporary.Chmod(0o600); err == nil {
		_, err = temporary.Write(append(encoded, '\n'))
	}
	if err == nil {
		err = temporary.Sync()
	}
	if closeErr := temporary.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	return os.Rename(temporaryName, filepath.Join(store.Directory, name))
}

func (store Store) load(name string, destination any) error {
	encoded, err := os.ReadFile(filepath.Join(store.Directory, name))
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(strings.NewReader(string(encoded)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return fmt.Errorf("healing artifact contains trailing data")
	}
	return nil
}

func validatePlanFields(plan Plan) error {
	if plan.Format != Format || plan.RepositoryID == "" || plan.SuspectGeneration == 0 || plan.CandidateGeneration <= plan.SuspectGeneration ||
		strings.TrimSpace(plan.CandidateNamespace) == "" ||
		plan.CreatedAt.IsZero() {
		return fmt.Errorf("invalid healing plan")
	}
	for _, source := range plan.Inventory.Sources {
		if source.ID == "" || source.Kind == "" || !source.Authenticated {
			return fmt.Errorf("healing source %q is not authenticated", source.ID)
		}
	}
	return nil
}

func signArtifact(value any, key []byte) (string, string) {
	encoded, _ := json.Marshal(value)
	digest := sha256.Sum256(encoded)
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write(digest[:]) // hash.Hash writes are specified to return a nil error.
	return hex.EncodeToString(digest[:]), hex.EncodeToString(mac.Sum(nil))
}

func cleanID(id string) string {
	if len(id) != 64 || strings.Trim(id, "0123456789abcdef") != "" {
		return "invalid"
	}
	return id
}
