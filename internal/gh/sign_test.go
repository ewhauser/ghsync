package gh

import "testing"

func TestSignatureRoundTrip(t *testing.T) {
	t.Parallel()
	secret := []byte("s3cret")
	body := []byte(`{"action":"stacked"}`)
	sig := SignBody(secret, body)
	if !VerifySignature(secret, body, sig) {
		t.Fatal("valid signature rejected")
	}
}

func TestSignatureMatchesGitHubTestVector(t *testing.T) {
	t.Parallel()
	const want = "sha256=" +
		"757107ea0eb2509fc211221cce984b8a37570b6d7586c22c46f4379c8b043e17"
	got := SignBody(
		[]byte("It's a Secret to Everybody"),
		[]byte("Hello, World!"),
	)
	if got != want {
		t.Fatalf("signature = %q, want %q", got, want)
	}
}

func TestSignatureRejectsTampering(t *testing.T) {
	t.Parallel()
	secret := []byte("s3cret")
	body := []byte(`{"action":"stacked"}`)
	sig := SignBody(secret, body)
	if VerifySignature(secret, []byte(`{"action":"opened"}`), sig) {
		t.Fatal("tampered body accepted")
	}
	if VerifySignature([]byte("wrong"), body, sig) {
		t.Fatal("wrong secret accepted")
	}
	if VerifySignature(secret, body, "sha256=deadbeef") {
		t.Fatal("bogus signature accepted")
	}
}
