package billing

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"
)

type Event struct {
	ID       string
	Type     string
	Livemode *bool
}

func VerifyEvent(body []byte, signature string, secret []byte, now time.Time) (Event, error) {
	var timestamp string
	var signatures []string
	for part := range strings.SplitSeq(signature, ",") {
		key, value, _ := strings.Cut(strings.TrimSpace(part), "=")
		switch key {
		case "t":
			if timestamp != "" {
				return Event{}, errors.New("stripe signature has duplicate timestamps")
			}
			timestamp = value
		case "v1":
			signatures = append(signatures, value)
		}
	}
	seconds, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil || seconds < now.Add(-5*time.Minute).Unix() || seconds > now.Add(5*time.Minute).Unix() || len(secret) < 16 || len(body) > 1024*1024 {
		return Event{}, errors.New("stripe webhook signature is invalid or expired")
	}
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(timestamp + "."))
	mac.Write(body)
	valid := false
	for _, signature := range signatures {
		decoded, err := hex.DecodeString(signature)
		if err == nil && hmac.Equal(mac.Sum(nil), decoded) {
			valid = true
		}
	}
	var event Event
	if !valid || json.Unmarshal(body, &event) != nil || !validID(event.ID, "evt_") || !testMode(event.Livemode) || event.Type == "" || len(event.Type) > 128 {
		return Event{}, errors.New("stripe webhook is not a verified test event")
	}
	return event, nil
}

func (e Event) Relevant() bool {
	return strings.HasPrefix(e.Type, "customer.subscription.") || strings.HasPrefix(e.Type, "invoice.") || strings.HasPrefix(e.Type, "checkout.session.") || strings.HasPrefix(e.Type, "charge.dispute.") || e.Type == "charge.refunded" || strings.HasPrefix(e.Type, "refund.")
}
