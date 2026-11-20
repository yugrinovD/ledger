package auth

import "testing"

func TestGenerateAndHash(t *testing.T) {
	t.Parallel()
	plain, prefix, err := GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	if len(plain) < 20 || prefix == "" {
		t.Fatalf("unexpected key shape: %q / %q", plain, prefix)
	}
	first, err := Hash("pepper", plain)
	if err != nil {
		t.Fatal(err)
	}
	second, _ := Hash("pepper", plain)
	if string(first) != string(second) {
		t.Fatal("hash must be deterministic")
	}
	other, _ := Hash("other", plain)
	if string(first) == string(other) {
		t.Fatal("pepper must affect the hash")
	}
}

func TestHasScope(t *testing.T) {
	t.Parallel()
	scopes := []string{"transfers:write", "accounts:read"}
	if !HasScope(scopes, "transfers:write") {
		t.Fatal("expected scope")
	}
	if HasScope(scopes, "holds:write") {
		t.Fatal("unexpected scope")
	}
	if !HasScope([]string{"*"}, "anything") {
		t.Fatal("wildcard scope must match")
	}
}
