package auth

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
)

const KeyPrefix = "lk_"

func GenerateKey() (plain, displayPrefix string, err error) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", "", err
	}
	plain = KeyPrefix + base64.RawURLEncoding.EncodeToString(buf)
	prefixLen := len(KeyPrefix) + 8
	if len(plain) < prefixLen {
		prefixLen = len(plain)
	}
	return plain, plain[:prefixLen], nil
}

// Hash derives a deterministic, peppered lookup value. The plaintext key is
// never stored in PostgreSQL.
func Hash(pepper, plain string) ([]byte, error) {
	if pepper == "" {
		return nil, errors.New("auth pepper must not be empty")
	}
	mac := hmac.New(sha256.New, []byte(pepper))
	mac.Write([]byte(plain))
	return mac.Sum(nil), nil
}

func HasScope(scopes []string, required string) bool {
	for _, s := range scopes {
		if s == required || s == "*" {
			return true
		}
	}
	return false
}
