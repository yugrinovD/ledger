package outbox

import (
	"strings"
	"testing"
	"unicode/utf8"
)

func TestTruncateKeepsValidUTF8(t *testing.T) {
	input := strings.Repeat("привет", 100)
	got := truncate(input, 11)
	if !utf8.ValidString(got) {
		t.Fatalf("truncate returned invalid UTF-8: %q", got)
	}
	if !strings.HasSuffix(got, "...") {
		t.Fatalf("truncate must mark the cut: %q", got)
	}
}

func TestTruncateShortStringUnchanged(t *testing.T) {
	if got := truncate("ok", 10); got != "ok" {
		t.Fatalf("truncate = %q", got)
	}
}
