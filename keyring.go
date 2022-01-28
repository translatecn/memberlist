package memberlist

import (
	"bytes"
	"fmt"
	"sync"
)

type Keyring struct {
	// Keys stores the key data used during encryption and decryption. It is
	// ordered in such a way where the first key (index 0) is the primary key,
	// which is used for encrypting messages, and is the first key tried during
	// message decryption.
	keys [][]byte

	// The keyring lock is used while performing IO operations on the keyring.
	l sync.Mutex
}

// 初始化分配了子结构
func (k *Keyring) init() {
	k.keys = make([][]byte, 0)
}

// NewKeyring 为一组加密密钥构造一个新容器。keyring包含成员列表内部使用的所有密钥数据。
func NewKeyring(keys [][]byte, primaryKey []byte) (*Keyring, error) {
	keyring := &Keyring{}
	keyring.init()

	if len(keys) > 0 || len(primaryKey) > 0 {
		if len(primaryKey) == 0 {
			return nil, fmt.Errorf("不允许空主秘钥")
		}
		if err := keyring.AddKey(primaryKey); err != nil {
			return nil, err
		}
		for _, key := range keys {
			if err := keyring.AddKey(key); err != nil {
				return nil, err
			}
		}
	}

	return keyring, nil
}

// ValidateKey will check to see if the key is valid and returns an error if not.
//
// key should be either 16, 24, or 32 bytes to select AES-128,
// AES-192, or AES-256.
func ValidateKey(key []byte) error {
	if l := len(key); l != 16 && l != 24 && l != 32 {
		return fmt.Errorf("key size must be 16, 24 or 32 bytes")
	}
	return nil
}

// AddKey 添加新的秘钥。如果在环上已经存在，这个函数将只返回noop。
func (k *Keyring) AddKey(key []byte) error {
	if err := ValidateKey(key); err != nil {
		return err
	}

	for _, installedKey := range k.keys {
		if bytes.Equal(installedKey, key) {
			return nil
		}
	}

	keys := append(k.keys, key)
	primaryKey := k.GetPrimaryKey()
	if primaryKey == nil {
		primaryKey = key
	}
	k.installKeys(keys, primaryKey)
	return nil
}

// UseKey 将一个已存在的key设置为主秘钥
func (k *Keyring) UseKey(key []byte) error {
	for _, installedKey := range k.keys {
		if bytes.Equal(key, installedKey) {
			k.installKeys(k.keys, key)
			return nil
		}
	}
	return fmt.Errorf("Requested key is not in the keyring")
}

// RemoveKey drops a key from the keyring. This will return an error if the key
// requested for removal is currently at position 0 (primary key).
func (k *Keyring) RemoveKey(key []byte) error {
	if bytes.Equal(key, k.keys[0]) {
		return fmt.Errorf("Removing the primary key is not allowed")
	}
	for i, installedKey := range k.keys {
		if bytes.Equal(key, installedKey) {
			keys := append(k.keys[:i], k.keys[i+1:]...)
			k.installKeys(keys, k.keys[0])
		}
	}
	return nil
}

// 重新排序，让primaryKey排在第一位
func (k *Keyring) installKeys(keys [][]byte, primaryKey []byte) {
	// keys 所有秘钥，primaryKey也在其中
	k.l.Lock()
	defer k.l.Unlock()
	// 重新排序，让primaryKey排在第一位
	newKeys := [][]byte{primaryKey}
	for _, key := range keys {
		if !bytes.Equal(key, primaryKey) {
			newKeys = append(newKeys, key)
		}
	}
	k.keys = newKeys
}

// GetKeys 返回私钥数据集
func (k *Keyring) GetKeys() [][]byte {
	k.l.Lock()
	defer k.l.Unlock()

	return k.keys
}

// GetPrimaryKey 返回用于加密数据的主私钥
func (k *Keyring) GetPrimaryKey() (key []byte) {
	k.l.Lock()
	defer k.l.Unlock()

	if len(k.keys) > 0 {
		key = k.keys[0]
	}
	return
}
