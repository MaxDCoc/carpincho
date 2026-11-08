package store

import (
	"strings"
	"testing"
	"time"
)

func TestGlobMatch(t *testing.T) {
	tests := []struct {
		pattern, s string
		want       bool
	}{
		{"*", "", true},
		{"*", "anything", true},
		{"", "", true},
		{"", "x", false},
		{"hello", "hello", true},
		{"hello", "hellO", false},
		{"h?llo", "hello", true},
		{"h?llo", "hallo", true},
		{"h?llo", "hllo", false},
		{"h*llo", "hllo", true},
		{"h*llo", "heeeello", true},
		{"h*llo", "heeeelloX", false},
		{"h[ae]llo", "hello", true},
		{"h[ae]llo", "hallo", true},
		{"h[ae]llo", "hillo", false},
		{"h[^e]llo", "hallo", true},
		{"h[^e]llo", "hello", false},
		{"h[a-b]llo", "hbllo", true},
		{"h[a-b]llo", "hcllo", false},
		{"h[b-a]llo", "hallo", true}, // reversed ranges are swapped, like Redis
		{`a\*b`, "a*b", true},
		{`a\*b`, "axb", false},
		{`user:*:name`, "user:42:name", true},
		{`user:*:name`, "user:42:email", false},
		{"**a**", "xxaxx", true},
		{`trailing\`, `trailing\`, true},
	}
	for _, tt := range tests {
		if got := GlobMatch(tt.pattern, tt.s); got != tt.want {
			t.Errorf("GlobMatch(%q, %q) = %v, want %v", tt.pattern, tt.s, got, tt.want)
		}
	}
}

// A naive recursive matcher is exponential on this input. Ours must not be.
func TestGlobMatchIsNotExponential(t *testing.T) {
	start := time.Now()
	if GlobMatch("*a*a*a*a*a*a*a*a*a*b", strings.Repeat("a", 60)) {
		t.Fatal("should not match")
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("pathological pattern took %v", d)
	}
}
