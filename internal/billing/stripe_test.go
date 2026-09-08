package billing

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

type stripeFixture struct {
	responses map[string]string
	status    int
	mu        sync.Mutex
	posts     []url.Values
	keys      []string
}

type stripeFixtureTransport struct {
	t      *testing.T
	base   *url.URL
	client http.RoundTripper
}

func (r stripeFixtureTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	if request.URL.Host != "api.stripe.com" || request.URL.Scheme != "https" || request.Header.Get("Stripe-Version") != StripeAPIVersion {
		r.t.Error("unexpected Stripe API destination or version")
	}
	request = request.Clone(request.Context())
	request.URL.Scheme, request.URL.Host, request.Host = r.base.Scheme, r.base.Host, r.base.Host
	return r.client.RoundTrip(request)
}

func newStripeFixture(t *testing.T) (*stripeFixture, Provider) {
	t.Helper()
	f := &stripeFixture{responses: map[string]string{
		"/v1/account":                                `{"id":"acct_test"}`,
		"/v1/customers/cus_test":                     `{"id":"cus_test","livemode":false,"metadata":{"detent_organization_id":"org_test"}}`,
		"/v1/subscriptions":                          `{"has_more":false,"data":[{"id":"sub_test","customer":"cus_test","livemode":false,"status":"active","metadata":{"detent_organization_id":"org_test"},"collection_method":"charge_automatically","items":{"has_more":false,"data":[{"quantity":1,"current_period_end":1900000000,"price":{"id":"price_test","livemode":false,"active":true,"type":"recurring","billing_scheme":"per_unit","recurring":{"usage_type":"licensed"}}}]},"latest_invoice":{"id":"in_test","customer":"cus_test","livemode":false,"status":"paid","created":1800000000,"amount_paid":1000,"parent":{"type":"subscription_details","subscription_details":{"subscription":"sub_test"}}}}]}`,
		"/v1/invoice_payments":                       `{"has_more":false,"data":[{"invoice":"in_test","livemode":false,"status":"paid","amount_paid":1000,"payment":{"type":"payment_intent","payment_intent":"pi_test"}}]}`,
		"/v1/payment_intents/pi_test":                `{"id":"pi_test","customer":"cus_test","livemode":false,"latest_charge":"ch_test"}`,
		"/v1/charges/ch_test":                        `{"id":"ch_test","customer":"cus_test","livemode":false,"amount":1000,"amount_refunded":0,"disputed":false}`,
		"/v1/disputes":                               `{"has_more":false,"data":[{"charge":"ch_test","livemode":false,"status":"needs_response"}]}`,
		"/v1/prices/price_test":                      `{"id":"price_test","livemode":false,"active":true,"type":"recurring","billing_scheme":"per_unit","recurring":{"usage_type":"licensed"}}`,
		"/v1/checkout/sessions":                      `{"id":"cs_test_fixture","customer":"cus_test","livemode":false,"url":"https://checkout.stripe.com/c/pay/test_fixture","expires_at":1900000000}`,
		"/v1/billing_portal/configurations/bpc_test": `{"id":"bpc_test","active":true,"livemode":false}`,
		"/v1/billing_portal/sessions":                `{"id":"bps_test","customer":"cus_test","livemode":false,"url":"https://billing.stripe.com/p/session/test_fixture"}`,
	}}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		key, _, ok := r.BasicAuth()
		if !ok || key != "sk_test_fixture_2196" {
			t.Error("Stripe fixture received unexpected credentials")
		}
		if r.Method == http.MethodPost {
			if err := r.ParseForm(); err != nil {
				t.Error(err)
			}
			f.posts = append(f.posts, r.PostForm)
			f.keys = append(f.keys, r.Header.Get("Idempotency-Key"))
		}
		if r.URL.Path == "/v1/subscriptions" && (r.URL.Query().Get("customer") != "cus_test" || r.URL.Query().Get("status") != "all") {
			t.Error("subscription reconciliation did not use the bound customer")
		}
		if r.URL.Path == "/v1/invoice_payments" && r.URL.Query().Get("invoice") != "in_test" {
			t.Error("payments were not scoped to the current invoice")
		}
		if r.URL.Path == "/v1/disputes" && r.URL.Query().Get("charge") != "ch_test" {
			t.Error("disputes were not scoped to the invoice charge")
		}
		if f.status != 0 {
			w.WriteHeader(f.status)
			fmt.Fprint(w, "private-provider-response-sentinel")
			return
		}
		body, ok := f.responses[r.URL.Path]
		if !ok {
			t.Errorf("unexpected Stripe path %q", r.URL.Path)
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, body)
	}))
	t.Cleanup(server.Close)
	base, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	t.Cleanup(transport.CloseIdleConnections)
	provider, err := NewStripe(StripeConfig{APIKey: "sk_test_fixture_2196", Client: &http.Client{Transport: stripeFixtureTransport{t: t, base: base, client: transport}}})
	if err != nil {
		t.Fatal(err)
	}
	return f, provider
}

func fixtureBinding() Binding {
	return Binding{AccountID: "acct_test", CustomerID: "cus_test", OrganizationID: "org_test"}
}

func TestStripeReconcile(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name, path, before, after, status, hold string
		wantError                               bool
	}{
		{name: "paid active", status: "active"},
		{name: "partial refund", path: "/v1/charges/ch_test", before: `"amount_refunded":0`, after: `"amount_refunded":200`, status: "active"},
		{name: "full refund", path: "/v1/charges/ch_test", before: `"amount_refunded":0`, after: `"amount_refunded":1000`, status: "active", hold: "refunded"},
		{name: "past due", path: "/v1/subscriptions", before: `"status":"active"`, after: `"status":"past_due"`, status: "past_due"},
		{name: "canceled", path: "/v1/subscriptions", before: `"status":"active"`, after: `"status":"canceled"`, status: "canceled"},
		{name: "paused collection", path: "/v1/subscriptions", before: `"collection_method":"charge_automatically"`, after: `"collection_method":"charge_automatically","pause_collection":{"behavior":"void"}`, status: "unsupported_subscription"},
		{name: "wrong subscription metadata", path: "/v1/subscriptions", before: `"detent_organization_id":"org_test"`, after: `"detent_organization_id":"org_other"`, status: "unsupported_subscription"},
		{name: "wrong customer", path: "/v1/customers/cus_test", before: "org_test", after: "org_other", wantError: true},
		{name: "wrong account", path: "/v1/account", before: "acct_test", after: "acct_other", wantError: true},
		{name: "live subscription", path: "/v1/subscriptions", before: `"livemode":false`, after: `"livemode":true`, wantError: true},
		{name: "missing mode", path: "/v1/customers/cus_test", before: `"livemode":false`, after: `"livemode":null`, wantError: true},
		{name: "foreign invoice", path: "/v1/subscriptions", before: `"subscription":"sub_test"`, after: `"subscription":"sub_other"`, wantError: true},
		{name: "incomplete subscriptions", path: "/v1/subscriptions", before: `"has_more":false`, after: `"has_more":true`, wantError: true},
		{name: "incomplete payments", path: "/v1/invoice_payments", before: `"has_more":false`, after: `"has_more":true`, wantError: true},
		{name: "foreign payment", path: "/v1/invoice_payments", before: "in_test", after: "in_other", wantError: true},
		{name: "foreign intent", path: "/v1/payment_intents/pi_test", before: "cus_test", after: "cus_other", wantError: true},
		{name: "foreign charge", path: "/v1/charges/ch_test", before: "cus_test", after: "cus_other", wantError: true},
		{name: "unsupported payment", path: "/v1/invoice_payments", before: `"type":"payment_intent"`, after: `"type":"payment_record"`, wantError: true},
		{name: "quantity", path: "/v1/subscriptions", before: `"quantity":1`, after: `"quantity":2`, status: "unsupported_subscription"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			f, provider := newStripeFixture(t)
			if test.path != "" {
				f.responses[test.path] = strings.Replace(f.responses[test.path], test.before, test.after, 1)
			}
			snapshot, err := provider.Reconcile(t.Context(), fixtureBinding())
			if (err != nil) != test.wantError || snapshot.Status != test.status || snapshot.PaymentHold != test.hold {
				t.Fatalf("reconcile = %+v, %v", snapshot, err)
			}
		})
	}
}

func TestStripeDisputeRecovery(t *testing.T) {
	t.Parallel()
	for _, test := range []struct{ status, hold string }{
		{"needs_response", "disputed"}, {"under_review", "disputed"}, {"lost", "disputed"}, {"won", ""}, {"warning_closed", ""},
	} {
		t.Run(test.status, func(t *testing.T) {
			t.Parallel()
			f, provider := newStripeFixture(t)
			f.responses["/v1/charges/ch_test"] = strings.ReplaceAll(f.responses["/v1/charges/ch_test"], `"disputed":false`, `"disputed":true`)
			f.responses["/v1/disputes"] = strings.ReplaceAll(f.responses["/v1/disputes"], "needs_response", test.status)
			snapshot, err := provider.Reconcile(t.Context(), fixtureBinding())
			if err != nil || snapshot.PaymentHold != test.hold {
				t.Fatalf("dispute = %+v, %v", snapshot, err)
			}
		})
	}
}

func TestStripeSessions(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name, path, before, after string
		portal, wantError         bool
	}{
		{name: "checkout"},
		{name: "portal", portal: true},
		{name: "live price", path: "/v1/prices/price_test", before: `"livemode":false`, after: `"livemode":true`, wantError: true},
		{name: "inactive price", path: "/v1/prices/price_test", before: `"active":true`, after: `"active":false`, wantError: true},
		{name: "foreign checkout", path: "/v1/checkout/sessions", before: "cus_test", after: "cus_other", wantError: true},
		{name: "untrusted redirect", path: "/v1/checkout/sessions", before: "checkout.stripe.com", after: "attacker.test", wantError: true},
		{name: "live portal config", portal: true, path: "/v1/billing_portal/configurations/bpc_test", before: `"livemode":false`, after: `"livemode":true`, wantError: true},
		{name: "foreign portal", portal: true, path: "/v1/billing_portal/sessions", before: "cus_test", after: "cus_other", wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			f, provider := newStripeFixture(t)
			if test.path != "" {
				f.responses[test.path] = strings.Replace(f.responses[test.path], test.before, test.after, 1)
			}
			var session Session
			var err error
			if test.portal {
				session, err = provider.Portal(t.Context(), fixtureBinding(), "bpc_test", "https://detent.example.test/organization/billing")
			} else {
				session, err = provider.Checkout(t.Context(), CheckoutRequest{Binding: fixtureBinding(), PriceID: "price_test", IdempotencyKey: "request_test", ReturnURL: "https://detent.example.test/organization/billing", ExpiresAt: time.Unix(1900000000, 0)})
			}
			if (err != nil) != test.wantError {
				t.Fatalf("session = %+v, %v", session, err)
			}
			if err != nil {
				return
			}
			f.mu.Lock()
			defer f.mu.Unlock()
			if len(f.posts) != 1 || f.posts[0].Get("customer") != "cus_test" {
				t.Fatalf("posts = %v", f.posts)
			}
			if !test.portal && (f.keys[0] != "request_test" || f.posts[0].Get("mode") != "subscription" || f.posts[0].Get("line_items[0][price]") != "price_test" || f.posts[0].Get("subscription_data[metadata][detent_organization_id]") != "org_test") {
				t.Fatalf("checkout parameters = %v", f.posts)
			}
			if f.posts[0].Get("automatic_tax[enabled]") != "" || f.posts[0].Get("allow_promotion_codes") != "" {
				t.Fatal("checkout enabled unconfigured tax or discounts")
			}
		})
	}
}

func TestStripeFailures(t *testing.T) {
	t.Parallel()
	for _, key := range []string{"", "sk_live_secret_sentinel", "invalid_secret_sentinel"} {
		if _, err := NewStripe(StripeConfig{APIKey: key}); err == nil || key != "" && strings.Contains(err.Error(), key) {
			t.Fatal("invalid or live key was accepted or exposed")
		}
	}
	for _, status := range []int{http.StatusUnauthorized, http.StatusTooManyRequests, http.StatusInternalServerError, http.StatusFound} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			t.Parallel()
			f, provider := newStripeFixture(t)
			f.status = status
			if _, err := provider.Reconcile(t.Context(), fixtureBinding()); err == nil || strings.Contains(err.Error(), "sentinel") {
				t.Fatalf("provider error = %v", err)
			}
		})
	}
	f, provider := newStripeFixture(t)
	f.responses["/v1/account"] = strings.Repeat("x", 2*1024*1024+1)
	if _, err := provider.Reconcile(t.Context(), fixtureBinding()); err == nil {
		t.Fatal("oversized response accepted")
	}
	f.responses["/v1/account"] = "{"
	if _, err := provider.Reconcile(t.Context(), fixtureBinding()); err == nil {
		t.Fatal("malformed response accepted")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := provider.Reconcile(ctx, fixtureBinding()); err == nil {
		t.Fatal("canceled reconciliation succeeded")
	}
}

func TestStripeEmptyAndMultipleSubscriptions(t *testing.T) {
	t.Parallel()
	for _, count := range []int{0, 2} {
		t.Run(strconv.Itoa(count), func(t *testing.T) {
			t.Parallel()
			f, provider := newStripeFixture(t)
			var data struct {
				Data []json.RawMessage `json:"data"`
			}
			if err := json.Unmarshal([]byte(f.responses["/v1/subscriptions"]), &data); err != nil {
				t.Fatal(err)
			}
			if count == 0 {
				data.Data = nil
			} else {
				data.Data = append(data.Data, data.Data[0])
			}
			raw, err := json.Marshal(data)
			if err != nil {
				t.Fatal(err)
			}
			f.responses["/v1/subscriptions"] = string(raw)
			snapshot, err := provider.Reconcile(t.Context(), fixtureBinding())
			want := "free"
			if count > 1 {
				want = "multiple_subscriptions"
			}
			if err != nil || snapshot.Status != want {
				t.Fatalf("snapshot = %+v, %v", snapshot, err)
			}
		})
	}
}
