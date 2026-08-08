package secret

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strings"
)

// getVaultEncryptionKey evaluates environment-based security policy and returns the AES key.
func getVaultEncryptionKey() ([]byte, error) {
	keyHex := os.Getenv("RHIZOME_VAULT_KEY")
	if keyHex == "" {
		if os.Getenv("RHIZOME_ALLOW_PLAINTEXT_VAULT") == "1" {
			return nil, nil
		}
		return nil, fmt.Errorf("RHIZOME_VAULT_KEY not set. Plaintext vault storage is disabled by default. Set RHIZOME_ALLOW_PLAINTEXT_VAULT=1 for local development")
	}
	key, err := hex.DecodeString(keyHex)
	if err != nil {
		return nil, fmt.Errorf("RHIZOME_VAULT_KEY must be a valid hex string: %w", err)
	}
	if len(key) != 32 {
		return nil, fmt.Errorf("RHIZOME_VAULT_KEY must be exactly 32 bytes (64 hex characters), got %d bytes", len(key))
	}
	return key, nil
}

// EncryptVaultData encrypts plaintext securely if a key is provided and policy requires it.
func EncryptVaultData(plaintextJSON string) (string, error) {
	key, err := getVaultEncryptionKey()
	if err != nil {
		return "", err
	}
	if len(key) == 0 {
		// Vault defaults to allowing plaintext only if the explicit override is set
		if plaintextJSON == "" {
			return "", nil
		}
		return plaintextJSON, nil
	}

	if plaintextJSON == "" {
		return "", nil // keep empty values empty to not generate noisy ciphertexts for omitted config
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", err
	}
	ciphertext := gcm.Seal(nil, nonce, []byte(plaintextJSON), nil)
	return fmt.Sprintf("aead:%s:%s", base64.StdEncoding.EncodeToString(nonce), base64.StdEncoding.EncodeToString(ciphertext)), nil
}

// DecryptVaultData decrypts an AEAD-prefixed ciphertext or returns plaintext for legacy compat.
func DecryptVaultData(storedData string) (string, error) {
	if storedData == "" {
		return "", nil
	}

	key, keyErr := getVaultEncryptionKey()

	if !strings.HasPrefix(storedData, "aead:") {
		// Legacy plaintext or encryption disabled
		if keyErr != nil {
			return "", fmt.Errorf("vault misconfigured: %w", keyErr)
		}
		return storedData, nil
	}
	if keyErr != nil {
		return "", fmt.Errorf("vault contains encrypted data but key is invalid: %w", keyErr)
	}
	if len(key) == 0 {
		return "", fmt.Errorf("vault contains encrypted data but RHIZOME_VAULT_KEY is not set (override is active but cannot decrypt)")
	}
	parts := strings.SplitN(storedData, ":", 3)
	if len(parts) != 3 {
		return "", fmt.Errorf("invalid vault encrypted payload format")
	}
	nonce, err := base64.StdEncoding.DecodeString(parts[1])
	if err != nil {
		return "", fmt.Errorf("decode nonce: %w", err)
	}
	ciphertext, err := base64.StdEncoding.DecodeString(parts[2])
	if err != nil {
		return "", fmt.Errorf("decode ciphertext: %w", err)
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}
	plaintext, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return "", fmt.Errorf("decrypt vault data: %w", err)
	}
	return string(plaintext), nil
}
