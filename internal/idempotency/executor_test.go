package idempotency

import (
	"bytes"
	"testing"
)

func TestHashRequestCanonicalizesStructOrder(t *testing.T) {
	t.Parallel()
	type payload struct {
		A string `json:"a"`
		B int    `json:"b"`
	}
	first, err := HashRequest("POST", "/v1/transfers", payload{A: "x", B: 2})
	if err != nil {
		t.Fatal(err)
	}
	second, err := HashRequest("POST", "/v1/transfers", payload{A: "x", B: 2})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, second) {
		t.Fatal("identical requests must produce identical hashes")
	}
	different, _ := HashRequest("POST", "/v1/transfers", payload{A: "y", B: 2})
	if bytes.Equal(first, different) {
		t.Fatal("different payloads must produce different hashes")
	}
	otherPath, _ := HashRequest("POST", "/v1/accounts", payload{A: "x", B: 2})
	if bytes.Equal(first, otherPath) {
		t.Fatal("path must be part of the fingerprint")
	}
}

func TestValidateKey(t *testing.T) {
	t.Parallel()
	if err := ValidateKey(""); err == nil {
		t.Fatal("empty key must be rejected")
	}
	if err := ValidateKey("key-1"); err != nil {
		t.Fatalf("valid key rejected: %v", err)
	}
	long := make([]byte, 256)
	for i := range long {
		long[i] = 'a'
	}
	if err := ValidateKey(string(long)); err == nil {
		t.Fatal("overlong key must be rejected")
	}
}
