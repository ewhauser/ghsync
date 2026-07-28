// Package gh holds GitHub client plumbing shared by the sync engine and the
// fake GitHub test server.
package gh

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
)

const signaturePrefix = "sha256="

// SignBody computes the X-Hub-Signature-256 header value for a webhook body.
func SignBody(secret, body []byte) string {
	mac := hmac.New(sha256.New, secret)
	mac.Write(body)
	return signaturePrefix + hex.EncodeToString(mac.Sum(nil))
}

// VerifySignature reports whether header is a valid signature for body.
// Constant-time; unverifiable deliveries must be rejected before parsing
// (SYNC_ENGINE C-I2).
func VerifySignature(secret, body []byte, header string) bool {
	expected := SignBody(secret, body)
	return hmac.Equal([]byte(expected), []byte(header))
}
