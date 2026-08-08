package secret

import (
	"os"
	"strings"
	"testing"
)

func TestVault_MissingKeyNoOverride_FailClosed(t *testing.T) {
	t.Cleanup(func() { os.Unsetenv("RHIZOME_VAULT_KEY"); os.Unsetenv("RHIZOME_ALLOW_PLAINTEXT_VAULT") })
	os.Unsetenv("RHIZOME_VAULT_KEY")
	os.Unsetenv("RHIZOME_ALLOW_PLAINTEXT_VAULT")

	_, err := EncryptVaultData("test")
	if err == nil {
		t.Fatal("expected error when encrypting without key and override, got nil")
	}
	if !strings.Contains(err.Error(), "Plaintext vault storage is disabled by default") {
		t.Fatalf("expected clear override error message, got: %v", err)
	}
}

func TestVault_MissingKeyExplicitOverride_PlaintextAllowed(t *testing.T) {
	t.Cleanup(func() { os.Unsetenv("RHIZOME_VAULT_KEY"); os.Unsetenv("RHIZOME_ALLOW_PLAINTEXT_VAULT") })
	os.Unsetenv("RHIZOME_VAULT_KEY")
	os.Setenv("RHIZOME_ALLOW_PLAINTEXT_VAULT", "1")

	ciphertext, err := EncryptVaultData("test")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ciphertext != "test" {
		t.Fatalf("expected plaintext 'test', got %q", ciphertext)
	}

	plaintext, err := DecryptVaultData(ciphertext)
	if err != nil {
		t.Fatalf("unexpected error on decrypt: %v", err)
	}
	if plaintext != "test" {
		t.Fatalf("expected 'test', got %q", plaintext)
	}
}

func TestVault_InvalidKey_HardError(t *testing.T) {
	t.Cleanup(func() { os.Unsetenv("RHIZOME_VAULT_KEY"); os.Unsetenv("RHIZOME_ALLOW_PLAINTEXT_VAULT") })
	os.Unsetenv("RHIZOME_ALLOW_PLAINTEXT_VAULT")

	// Test invalid hex length
	os.Setenv("RHIZOME_VAULT_KEY", "deadbeef")
	_, err := EncryptVaultData("test")
	if err == nil {
		t.Fatal("expected error for invalid key length")
	}
	if !strings.Contains(err.Error(), "exactly 32 bytes") {
		t.Fatalf("expected 32 bytes error message, got: %v", err)
	}

	// Test invalid hex string
	os.Setenv("RHIZOME_VAULT_KEY", "invalidhexstringxxxxxxxxxxxxxxxxxxxxx")
	_, err = EncryptVaultData("test")
	if err == nil {
		t.Fatal("expected error for invalid hex string")
	}
}

func TestVault_EncryptedDataNoKey_Error(t *testing.T) {
	t.Cleanup(func() { os.Unsetenv("RHIZOME_VAULT_KEY"); os.Unsetenv("RHIZOME_ALLOW_PLAINTEXT_VAULT") })
	os.Setenv("RHIZOME_ALLOW_PLAINTEXT_VAULT", "1")
	os.Unsetenv("RHIZOME_VAULT_KEY")

	// Even if override is active, trying to read an AEAD ciphertext should fail explicitly!
	_, err := DecryptVaultData("aead:nonce:ciphertext")
	if err == nil {
		t.Fatal("expected error when trying to decrypt AEAD with no key")
	}
	if !strings.Contains(err.Error(), "override is active but cannot decrypt") {
		t.Fatalf("expected specific missing key decrypt error, got: %v", err)
	}
}

func TestVault_ValidKey_NormalPath(t *testing.T) {
	t.Cleanup(func() { os.Unsetenv("RHIZOME_VAULT_KEY"); os.Unsetenv("RHIZOME_ALLOW_PLAINTEXT_VAULT") })
	os.Unsetenv("RHIZOME_ALLOW_PLAINTEXT_VAULT")
	// 32-byte key (64 hex chars)
	keyHex := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	os.Setenv("RHIZOME_VAULT_KEY", keyHex)

	ciphertext, err := EncryptVaultData("top_secret_data")
	if err != nil {
		t.Fatalf("encrypt unexpected error: %v", err)
	}
	if !strings.HasPrefix(ciphertext, "aead:") {
		t.Fatalf("expected aead prefix in ciphertext, got %q", ciphertext)
	}

	plaintext, err := DecryptVaultData(ciphertext)
	if err != nil {
		t.Fatalf("decrypt unexpected error: %v", err)
	}
	if plaintext != "top_secret_data" {
		t.Fatalf("expected 'top_secret_data', got %q", plaintext)
	}
}
