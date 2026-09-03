package repository

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"os/user"
	"sync"
	"time"

	"github.com/otuschhoff/vaultic/internal/errors"
	"github.com/otuschhoff/vaultic/internal/vaultic"

	"github.com/otuschhoff/vaultic/internal/backend"
	"github.com/otuschhoff/vaultic/internal/debug"
	"github.com/otuschhoff/vaultic/internal/repository/crypto"
)

var (
	// ErrNoKeyFound is returned when no key for the repository could be decrypted.
	ErrNoKeyFound = errors.New("wrong password or no key found")

	// ErrMaxKeysReached is returned when the maximum number of keys was checked and no key could be found.
	ErrMaxKeysReached = errors.New("maximum number of keys reached")
)

// Key represents an encrypted master key for a repository.
type Key struct {
	Created  time.Time `json:"created"`
	Username string    `json:"username"`
	Hostname string    `json:"hostname"`

	KDF  string `json:"kdf"`
	N    int    `json:"N"`
	R    int    `json:"r"`
	P    int    `json:"p"`
	Salt []byte `json:"salt"`
	Data []byte `json:"data"`

	user   *crypto.Key
	master *crypto.Key

	id vaultic.ID
}

// params tracks the parameters used for the KDF. If not set, it will be
// calibrated on the first run of AddKey().
var params *crypto.Params

// testKeyInjection is used to speed up tests by skipping the key decryption step.
var testKeyInjection = sync.Map{}

const (
	// KDFTimeout specifies the maximum runtime for the KDF.
	KDFTimeout = 500 * time.Millisecond

	// KDFMemory limits the memory the KDF is allowed to use.
	KDFMemory = 60
)

// createMasterKey creates a new master key in the given backend and encrypts
// it with the password.
func createMasterKey(ctx context.Context, s *Repository, password string) (*Key, error) {
	return AddKey(ctx, s, password, "", "", nil)
}

// openKey tries do decrypt the key specified by name with the given password.
func openKey(ctx context.Context, s *Repository, id vaultic.ID, password string) (*Key, error) {
	if key, ok := testKeyInjection.Load(id); ok {
		return &Key{
			master: key.(*crypto.Key),
			user:   key.(*crypto.Key), // not correct but good enough for testing
			id:     id,
		}, nil
	}

	k, err := LoadKey(ctx, s, id)
	if err != nil {
		debug.Log("LoadKey(%v) returned error %v", id.String(), err)
		return nil, err
	}

	// check KDF
	if k.KDF != "scrypt" {
		return nil, errors.New("only supported KDF is scrypt()")
	}

	// derive user key
	params := crypto.Params{
		N: k.N,
		R: k.R,
		P: k.P,
	}
	k.user, err = crypto.KDF(params, k.Salt, password)
	if err != nil {
		return nil, errors.Wrap(err, "crypto.KDF")
	}

	// decrypt master keys
	if len(k.Data) < crypto.CiphertextLength(0) {
		return nil, errors.New("invalid key file, data too short")
	}

	nonce, ciphertext := k.Data[:k.user.NonceSize()], k.Data[k.user.NonceSize():]
	buf, err := k.user.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, err
	}

	// restore json
	k.master = &crypto.Key{}
	err = json.Unmarshal(buf, k.master)
	if err != nil {
		debug.Log("Unmarshal() returned error %v", err)
		return nil, errors.Wrap(err, "Unmarshal")
	}
	k.id = id

	if !k.Valid() {
		return nil, errors.New("Invalid key for repository")
	}

	return k, nil
}

// searchKey tries to decrypt at most maxKeys keys in the backend with the
// given password. If none could be found, ErrNoKeyFound is returned. When
// maxKeys is reached, ErrMaxKeysReached is returned. When setting maxKeys to
// zero, all keys in the repo are checked.
func searchKey(ctx context.Context, s *Repository, password string, maxKeys int, keyHint string) (k *Key, err error) {
	checked := 0

	if len(keyHint) > 0 {
		id, err := vaultic.Find(ctx, s, vaultic.KeyFile, keyHint)

		if err == nil {
			key, err := openKey(ctx, s, id, password)

			if err == nil {
				debug.Log("successfully opened hinted key %v", id)
				return key, nil
			}

			debug.Log("could not open hinted key %v", id)
		} else {
			debug.Log("Could not find hinted key %v", keyHint)
		}
	}

	listCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// try at most maxKeys keys in repo
	err = s.List(listCtx, vaultic.KeyFile, func(id vaultic.ID, _ int64) error {
		checked++
		if maxKeys > 0 && checked > maxKeys {
			return ErrMaxKeysReached
		}

		debug.Log("trying key %q", id.String())
		key, err := openKey(ctx, s, id, password)
		if err != nil {
			debug.Log("key %v returned error %v", id.String(), err)

			// ErrUnauthenticated means the password is wrong, try the next key
			if errors.Is(err, crypto.ErrUnauthenticated) {
				return nil
			}

			return err
		}

		debug.Log("successfully opened key %v", id.String())
		k = key
		cancel()
		return nil
	})

	if err == context.Canceled {
		err = nil
	}

	if err != nil {
		return nil, err
	}

	if k == nil {
		return nil, ErrNoKeyFound
	}

	return k, nil
}

// UseMasterKey opens the repository using a master key directly, bypassing
// the password-based key files. masterKeyJSON is the base64-encoded JSON
// representation of the master key (as printed by `vaultic cat masterkey`).
//
// This is the vaultic equivalent of rustic's `--key` option. Handle the key
// with care: anyone in possession of it can decrypt the whole repository.
func (r *Repository) UseMasterKey(ctx context.Context, masterKeyJSON string) error {
	key, err := DecodeMasterKey(masterKeyJSON)
	if err != nil {
		return err
	}

	r.key = key
	// no key file is associated with a master key; use a zero ID
	r.keyID = vaultic.ID{}

	cfg, err := vaultic.LoadConfig(ctx, r)
	if err != nil {
		return fmt.Errorf("config cannot be loaded: %w", err)
	}
	r.setConfig(cfg)
	r.ApplyRepoSettings()
	return nil
}

func (r *Repository) UseDataPlaneMasterKey(masterKeyJSON string, cfg vaultic.Config) error {
	key, err := DecodeMasterKey(masterKeyJSON)
	if err != nil {
		return err
	}
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("invalid bootstrap config projection: %w", err)
	}
	r.key = key
	r.keyID = vaultic.ID{}
	r.setConfig(cfg)
	r.ApplyRepoSettings()
	return nil
}

// DecodeMasterKey validates the base64-encoded repository key without opening a backend.
func DecodeMasterKey(masterKeyJSON string) (*crypto.Key, error) {
	raw, err := base64.StdEncoding.DecodeString(masterKeyJSON)
	if err != nil {
		return nil, errors.Fatalf("invalid master key (expected base64-encoded JSON): %v", err)
	}

	key := &crypto.Key{}
	if err := json.Unmarshal(raw, key); err != nil {
		return nil, errors.Fatalf("invalid master key (expected JSON): %v", err)
	}
	if !key.Valid() {
		return nil, errors.Fatal("invalid master key")
	}
	return key, nil
}

// LoadKey loads a key from the backend.
func LoadKey(ctx context.Context, s *Repository, id vaultic.ID) (k *Key, err error) {
	data, err := s.LoadRaw(ctx, vaultic.KeyFile, id)
	if err != nil {
		return nil, err
	}

	k = &Key{}
	err = json.Unmarshal(data, k)
	if err != nil {
		return nil, errors.Wrap(err, "Unmarshal")
	}

	return k, nil
}

// AddKey adds a new key to an already existing repository.
func AddKey(ctx context.Context, s *Repository, password, username, hostname string, template *crypto.Key) (*Key, error) {
	// make sure we have valid KDF parameters
	if params == nil {
		p, err := crypto.Calibrate(KDFTimeout, KDFMemory)
		if err != nil {
			return nil, errors.Wrap(err, "Calibrate")
		}

		params = &p
		debug.Log("calibrated KDF parameters are %v", p)
	}

	// fill meta data about key
	newkey := &Key{
		Created:  time.Now(),
		Username: username,
		Hostname: hostname,

		KDF: "scrypt",
		N:   params.N,
		R:   params.R,
		P:   params.P,
	}

	if newkey.Hostname == "" {
		newkey.Hostname, _ = os.Hostname()
	}

	if newkey.Username == "" {
		usr, err := user.Current()
		if err == nil {
			newkey.Username = usr.Username
		}
	}

	// generate random salt
	var err error
	newkey.Salt, err = crypto.NewSalt()
	if err != nil {
		panic("unable to read enough random bytes for salt: " + err.Error())
	}

	// call KDF to derive user key
	newkey.user, err = crypto.KDF(*params, newkey.Salt, password)
	if err != nil {
		return nil, err
	}

	if template == nil {
		// generate new random master keys
		newkey.master = crypto.NewRandomKey()
	} else {
		// copy master keys from old key
		newkey.master = template
	}

	// encrypt master keys (as json) with user key
	buf, err := json.Marshal(newkey.master)
	if err != nil {
		return nil, errors.Wrap(err, "Marshal")
	}

	nonce := crypto.NewRandomNonce()
	ciphertext := make([]byte, 0, crypto.CiphertextLength(len(buf)))
	ciphertext = append(ciphertext, nonce...)
	ciphertext = newkey.user.Seal(ciphertext, nonce, buf, nil)
	newkey.Data = ciphertext

	// dump as json
	buf, err = json.Marshal(newkey)
	if err != nil {
		return nil, errors.Wrap(err, "Marshal")
	}

	id := vaultic.Hash(buf)
	// store in repository and return
	h := backend.Handle{Type: backend.KeyFile, Name: id.String()}

	err = s.be.Save(ctx, h, backend.NewByteReader(buf, s.be.Hasher()))
	if err != nil {
		return nil, err
	}

	newkey.id = id

	return newkey, nil
}

func RemoveKey(ctx context.Context, repo *Repository, id vaultic.ID) error {
	if id == repo.KeyID() {
		return errors.New("refusing to remove key currently used to access repository")
	}

	h := backend.Handle{Type: backend.KeyFile, Name: id.String()}
	return repo.be.Remove(ctx, h)
}

func (k *Key) String() string {
	if k == nil {
		return "<Key nil>"
	}
	return fmt.Sprintf("<Key of %s@%s, created on %s>", k.Username, k.Hostname, k.Created)
}

// ID returns an identifier for the key.
func (k Key) ID() vaultic.ID {
	return k.id
}

// Valid tests whether the mac and encryption keys are valid (i.e. not zero)
func (k *Key) Valid() bool {
	return k.user.Valid() && k.master.Valid()
}
