package auth

import (
	"encoding/base64"
	"strings"
	"testing"
)

// T-1: Verifies R-1, R-3: output is 86 characters.
func TestGenerateCodeVerifier_Length(t *testing.T) {
	v, err := GenerateCodeVerifier()
	if err != nil {
		t.Fatalf("GenerateCodeVerifier() error: %v", err)
	}
	if len(v) != 86 {
		t.Errorf("expected length 86, got %d", len(v))
	}
}

// T-2: Verifies R-4: output decodes with base64.RawURLEncoding without error.
func TestGenerateCodeVerifier_Base64URL(t *testing.T) {
	v, err := GenerateCodeVerifier()
	if err != nil {
		t.Fatalf("GenerateCodeVerifier() error: %v", err)
	}
	_, err = base64.RawURLEncoding.DecodeString(v)
	if err != nil {
		t.Errorf("output is not valid base64url: %v", err)
	}
}

// T-3: Verifies R-1: two calls produce different values.
func TestGenerateCodeVerifier_Uniqueness(t *testing.T) {
	v1, err := GenerateCodeVerifier()
	if err != nil {
		t.Fatalf("first GenerateCodeVerifier() error: %v", err)
	}
	v2, err := GenerateCodeVerifier()
	if err != nil {
		t.Fatalf("second GenerateCodeVerifier() error: %v", err)
	}
	if v1 == v2 {
		t.Error("two calls produced identical verifiers")
	}
}

// T-4: Verifies R-2: known test vector from RFC 7636 Appendix B.
func TestCodeChallenge_KnownVector(t *testing.T) {
	// RFC 7636 Appendix B test vector.
	verifier := "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"
	expected := "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM"

	got := CodeChallenge(verifier)
	if got != expected {
		t.Errorf("CodeChallenge(%q) = %q, want %q", verifier, got, expected)
	}
}

// T-5: Verifies R-4: output contains no '=' characters.
func TestCodeChallenge_NoPadding(t *testing.T) {
	v, err := GenerateCodeVerifier()
	if err != nil {
		t.Fatalf("GenerateCodeVerifier() error: %v", err)
	}
	challenge := CodeChallenge(v)
	if strings.Contains(challenge, "=") {
		t.Errorf("challenge contains padding character '=': %q", challenge)
	}
}

// T-6: Verifies R-5: output is 43 characters.
func TestGenerateState_Length(t *testing.T) {
	s, err := GenerateState()
	if err != nil {
		t.Fatalf("GenerateState() error: %v", err)
	}
	if len(s) != 43 {
		t.Errorf("expected length 43, got %d", len(s))
	}
}
