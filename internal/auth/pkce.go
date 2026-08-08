package auth

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
)

// GenerateCodeVerifier generates a cryptographically random code_verifier
// string per RFC 7636. It uses crypto/rand to produce 64 random bytes,
// then encodes them as base64url with no padding (86 characters).
func GenerateCodeVerifier() (string, error) {
	buf := make([]byte, 64)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("generating code verifier: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}

// CodeChallenge computes the S256 code_challenge from a code_verifier.
// It SHA-256 hashes the verifier bytes, then base64url encodes the hash
// with no padding (43 characters).
func CodeChallenge(verifier string) string {
	h := sha256.Sum256([]byte(verifier))
	return base64.RawURLEncoding.EncodeToString(h[:])
}

// GenerateState generates a random state parameter for CSRF protection.
// It produces 32 random bytes and encodes them as base64url with no padding
// (43 characters).
func GenerateState() (string, error) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("generating state: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}
