package workingkv

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sync"
)

const secureChunkBytes = 128 << 10

type SecureMap struct {
	*WorkingStore
	mutex  sync.Mutex
	aead   cipher.AEAD
	key    []byte
	domain []byte
}

func OpenSecureMap(ctx context.Context, parent, prefix string) (*SecureMap, error) {
	key := make([]byte, 64)
	if _, err := rand.Read(key); err != nil {
		return nil, err
	}
	block, err := aes.NewCipher(key[:32])
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	store, err := OpenWorking(ctx, parent, prefix)
	if err != nil {
		return nil, err
	}
	return &SecureMap{WorkingStore: store, aead: aead, key: key[32:], domain: []byte("vaultic-working/v1/" + prefix)}, nil
}

func (store *SecureMap) token(key []byte) []byte {
	mac := hmac.New(sha256.New, store.key)
	_, _ = mac.Write(key)
	return mac.Sum(nil)
}

func (store *SecureMap) encode(key, value []byte) ([]byte, error) {
	nonce := make([]byte, store.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	return store.aead.Seal(nonce, nonce, value, append(append([]byte(nil), store.domain...), key...)), nil
}

func (store *SecureMap) decode(key, value []byte) ([]byte, error) {
	if len(value) < store.aead.NonceSize() {
		return nil, fmt.Errorf("truncated working-state ciphertext")
	}
	return store.aead.Open(nil, value[:store.aead.NonceSize()], value[store.aead.NonceSize():], append(append([]byte(nil), store.domain...), key...))
}

func (store *SecureMap) PutValue(ctx context.Context, key, value []byte) error {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	root := store.token(key)
	var header []byte
	if len(value) <= secureChunkBytes {
		header = append([]byte{0}, value...)
	} else {
		header = make([]byte, 25)
		header[0] = 1
		binary.BigEndian.PutUint64(header[1:9], uint64(len(value)))
		if _, err := rand.Read(header[9:]); err != nil {
			return err
		}
		for offset, ordinal := 0, uint64(0); offset < len(value); ordinal++ {
			chunkKey := append(append([]byte(nil), root...), header[9:]...)
			chunkKey = binary.BigEndian.AppendUint64(chunkKey, ordinal)
			last := min(offset+secureChunkBytes, len(value))
			encoded, err := store.encode(chunkKey, value[offset:last])
			if err != nil {
				return err
			}
			if err := store.WorkingStore.Put(ctx, []Entry{{Key: chunkKey, Value: encoded}}); err != nil {
				return err
			}
			offset = last
		}
	}
	encoded, err := store.encode(root, header)
	if err != nil {
		return err
	}
	return store.WorkingStore.Put(ctx, []Entry{{Key: root, Value: encoded}})
}

func (store *SecureMap) GetValue(ctx context.Context, key []byte) ([]byte, bool, error) {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	root := store.token(key)
	encoded, found, err := store.WorkingStore.Get(ctx, root)
	if err != nil || !found {
		return nil, found, err
	}
	header, err := store.decode(root, encoded)
	if err != nil {
		return nil, false, err
	}
	if len(header) == 0 {
		return nil, false, fmt.Errorf("empty working-state record")
	}
	if header[0] == 0 {
		return header[1:], true, nil
	}
	if header[0] != 1 || len(header) != 25 {
		return nil, false, fmt.Errorf("invalid working-state chunk header")
	}
	size := binary.BigEndian.Uint64(header[1:9])
	if size > uint64(^uint(0)>>1) {
		return nil, false, ErrBatchLimit
	}
	var value []byte
	for ordinal := uint64(0); uint64(len(value)) < size; ordinal++ {
		chunkKey := append(append([]byte(nil), root...), header[9:]...)
		chunkKey = binary.BigEndian.AppendUint64(chunkKey, ordinal)
		encoded, found, err := store.WorkingStore.Get(ctx, chunkKey)
		if err != nil {
			return nil, false, err
		}
		if !found {
			return nil, false, fmt.Errorf("missing working-state chunk")
		}
		chunk, err := store.decode(chunkKey, encoded)
		if err != nil {
			return nil, false, err
		}
		if uint64(len(chunk)) != min(uint64(secureChunkBytes), size-uint64(len(value))) {
			return nil, false, fmt.Errorf("invalid working-state chunk length")
		}
		value = append(value, chunk...)
	}
	return value, true, nil
}

func (store *SecureMap) Close() error {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	err := store.WorkingStore.Close()
	clear(store.key)
	return err
}
