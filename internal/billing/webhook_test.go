package billing

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"strings"
	"testing"
	"time"
)

func stripeSignature(body, secret string, now time.Time) string {
	stamp := strconv.FormatInt(now.Unix(), 10)
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(stamp + "." + body))
	return "t=" + stamp + ",v1=" + hex.EncodeToString(mac.Sum(nil))
}

func TestVerifyStripeEvent(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)
	secret := "whsec_fixture_2196_secret"
	valid := `{"id":"evt_test","type":"invoice.paid","livemode":false}`
	for _, test := range []struct {
		name, body, signature string
		offset                time.Duration
		wantError             bool
	}{
		{name: "valid", body: valid},
		{name: "rotated signature", body: valid, signature: stripeSignature(valid, secret, now) + ",v1=00"},
		{name: "wrong signature", body: valid, signature: stripeSignature(valid, "another_secret", now), wantError: true},
		{name: "changed body", body: valid + " ", signature: stripeSignature(valid, secret, now), wantError: true},
		{name: "old", body: valid, offset: -5*time.Minute - time.Second, wantError: true},
		{name: "future", body: valid, offset: 5*time.Minute + time.Second, wantError: true},
		{name: "boundary", body: valid, offset: -5 * time.Minute},
		{name: "live event", body: strings.ReplaceAll(valid, "false", "true"), wantError: true},
		{name: "missing mode", body: strings.ReplaceAll(valid, "false", "null"), wantError: true},
		{name: "missing ID", body: strings.ReplaceAll(valid, "evt_test", ""), wantError: true},
		{name: "malformed", body: "{", wantError: true},
		{name: "duplicate timestamp", body: valid, signature: stripeSignature(valid, secret, now) + ",t=1", wantError: true},
		{name: "missing timestamp", body: valid, signature: "v1=00", wantError: true},
		{name: "oversized", body: strings.Repeat("x", 1024*1024+1), wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			signature := test.signature
			if signature == "" {
				signature = stripeSignature(test.body, secret, now.Add(test.offset))
			}
			event, err := VerifyEvent([]byte(test.body), signature, []byte(secret), now)
			if (err != nil) != test.wantError {
				t.Fatalf("event=%+v error=%v", event, err)
			}
			if err == nil && !event.Relevant() {
				t.Fatal("invoice event was ignored")
			}
		})
	}
	for _, test := range []struct {
		kind     string
		relevant bool
	}{
		{"customer.subscription.deleted", true}, {"charge.dispute.closed", true}, {"charge.refunded", true}, {"refund.updated", true}, {"checkout.session.completed", true}, {"customer.created", false},
	} {
		if (Event{Type: test.kind}).Relevant() != test.relevant {
			t.Errorf("relevance=%s", test.kind)
		}
	}
}
