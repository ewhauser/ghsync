package gh

import "testing"

func TestSignatureRoundTrip(t *testing.T) {
	secret := []byte("s3cret")
	body := []byte(`{"action":"stacked"}`)
	sig := SignBody(secret, body)
	if !VerifySignature(secret, body, sig) {
		t.Fatal("valid signature rejected")
	}
}

func TestSignatureRejectsTampering(t *testing.T) {
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
