package hubserver

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/digitaldrywood/detent/internal/billing"
)

type hostedBillingProvider struct {
	mu           sync.Mutex
	snapshot     billing.Snapshot
	fail         bool
	checkoutFail bool
	calls        int
	checkouts    []billing.CheckoutRequest
	portals      []billing.Binding
	during       func()
}

func (p *hostedBillingProvider) Reconcile(_ context.Context, binding billing.Binding) (billing.Snapshot, error) {
	p.mu.Lock()
	p.calls++
	snapshot, fail, during := p.snapshot, p.fail, p.during
	p.mu.Unlock()
	if binding.CustomerID != "cus_fixture" || binding.OrganizationID != "org_browser_preview" {
		return billing.Snapshot{}, errors.New("incorrect organization binding")
	}
	if during != nil {
		during()
	}
	if fail {
		return billing.Snapshot{}, errors.New("fixture Stripe outage")
	}
	return snapshot, nil
}

func (p *hostedBillingProvider) Checkout(_ context.Context, request billing.CheckoutRequest) (billing.Session, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.checkouts = append(p.checkouts, request)
	if p.checkoutFail {
		return billing.Session{}, errors.New("fixture uncertain checkout response")
	}
	return billing.Session{ID: "cs_test_fixture", URL: "https://checkout.stripe.com/c/pay/test_fixture", ExpiresAt: request.ExpiresAt}, nil
}

func (p *hostedBillingProvider) Portal(_ context.Context, binding billing.Binding, _, _ string) (billing.Session, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.portals = append(p.portals, binding)
	if p.fail {
		return billing.Session{}, errors.New("fixture Stripe outage")
	}
	return billing.Session{ID: "bps_fixture", URL: "https://billing.stripe.com/p/session/test_fixture"}, nil
}

func newHostedBillingFixture(t *testing.T) (*browserHostedFixture, *hostedBillingProvider) {
	t.Helper()
	f := newBrowserHostedFixture(t, true)
	plans := hostedTestPlans(t, f.service, map[string]int64{"projects": 2})
	p := &hostedBillingProvider{snapshot: billing.Snapshot{Status: "free"}}
	f.service.config.Hosted.Plans = &plans
	f.service.config.Hosted.Billing = &HostedBillingConfig{
		AccountID: "acct_fixture", CustomerID: "cus_fixture", PortalConfigurationID: "bpc_fixture", WebhookSecret: []byte("whsec_fixture_2196_secret"), GraceSeconds: 3600, ReconcileSeconds: 60, Provider: p,
		Prices: []HostedBillingPrice{{PriceID: "price_fixture", Label: "Extended pilot", Plan: plans.Plans[1].PlanReference}},
	}
	if err := f.service.config.Hosted.validate(); err != nil {
		t.Fatal(err)
	}
	if err := f.service.database.configureHostedBilling(t.Context(), f.service.config.Hosted); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	close(done)
	f.service.billing = &hostedBillingWorker{service: f.service, cancel: func() {}, done: done}
	return f, p
}

func activeBillingSnapshot(now time.Time) billing.Snapshot {
	return billing.Snapshot{SubscriptionID: "sub_fixture", PriceID: "price_fixture", Status: "active", InvoiceID: "in_fixture", InvoiceStatus: "paid", InvoiceCreatedAt: now.Add(-time.Hour), PeriodEnd: now.Add(time.Hour)}
}

func postBillingEvent(t *testing.T, f *browserHostedFixture, id, kind string, signed bool) *httptest.ResponseRecorder {
	t.Helper()
	body := fmt.Sprintf(`{"id":%q,"type":%q,"livemode":false,"data":{"object":{"customer":"cus_other","status":"active","private":"card-sentinel"}}}`, id, kind)
	stamp := strconv.FormatInt(f.service.config.now().Unix(), 10)
	mac := hmac.New(sha256.New, f.service.config.Hosted.Billing.WebhookSecret)
	mac.Write([]byte(stamp + "." + body))
	r := httptest.NewRequest(http.MethodPost, "/webhooks/stripe", strings.NewReader(body))
	if signed {
		r.Header.Set("Stripe-Signature", "t="+stamp+",v1="+hex.EncodeToString(mac.Sum(nil)))
	}
	w := httptest.NewRecorder()
	f.service.Handler().ServeHTTP(w, r)
	return w
}

func TestHostedBillingLifecycle(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)
	config := &HostedBillingConfig{GraceSeconds: 3600, Prices: []HostedBillingPrice{{PriceID: "price_fixture", Plan: PlanReference{ID: "paid", Version: 1}}}}
	previous := resolveHostedBilling(config, hostedBillingState{}, activeBillingSnapshot(now), now)
	for _, test := range []struct {
		name, status, invoice, hold string
		cancel                      bool
		at                          time.Duration
		want                        string
		access                      bool
	}{
		{"activation", "active", "paid", "", false, 0, "active", true},
		{"incomplete", "incomplete", "open", "", false, 0, "incomplete", false},
		{"payment failure", "past_due", "open", "", false, time.Hour, "grace", true},
		{"exact grace expiry", "past_due", "open", "", false, 2 * time.Hour, "payment_failed", false},
		{"active unpaid invoice", "active", "open", "", false, time.Hour, "grace", true},
		{"cancellation scheduled", "active", "paid", "", true, 0, "canceling", true},
		{"cancellation boundary", "active", "paid", "", true, time.Hour, "canceling", false},
		{"immediate cancellation", "canceled", "paid", "", false, 0, "canceled", false},
		{"unpaid", "unpaid", "open", "", false, 0, "unpaid", false},
		{"paused", "paused", "paid", "", false, 0, "paused", false},
		{"refund", "active", "paid", "refunded", false, 0, "refunded", false},
		{"dispute", "active", "paid", "disputed", false, 0, "disputed", false},
		{"recovery", "active", "paid", "", false, 0, "active", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			snapshot := activeBillingSnapshot(now)
			snapshot.Status, snapshot.InvoiceStatus, snapshot.PaymentHold, snapshot.CancelAtPeriodEnd = test.status, test.invoice, test.hold, test.cancel
			at := now.Add(test.at)
			state := resolveHostedBilling(config, previous, snapshot, at)
			if state.Status != test.want || state.AccessUntil.After(at) != test.access {
				t.Fatalf("state=%+v", state)
			}
			if test.status == "past_due" && !state.GraceUntil.Equal(previous.PaidThrough.Add(time.Hour)) {
				t.Fatal("failed payment extended grace")
			}
		})
	}
	for _, test := range []struct {
		name     string
		snapshot billing.Snapshot
		previous hostedBillingState
		want     bool
	}{
		{"unapproved price", billing.Snapshot{SubscriptionID: "sub_fixture", PriceID: "price_other", Status: "active"}, previous, false},
		{"failed initial payment", billing.Snapshot{SubscriptionID: "sub_new", PriceID: "price_fixture", Status: "past_due"}, previous, false},
		{"trial", billing.Snapshot{SubscriptionID: "sub_trial", PriceID: "price_fixture", Status: "trialing", TrialEnd: now.Add(time.Hour), PeriodEnd: now.Add(time.Hour)}, hostedBillingState{}, true},
		{"free", billing.Snapshot{Status: "free"}, previous, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			state := resolveHostedBilling(config, test.previous, test.snapshot, now)
			if state.AccessUntil.After(now) != test.want {
				t.Fatalf("state=%+v", state)
			}
		})
	}
}

func TestHostedBillingReplayAndRecovery(t *testing.T) {
	t.Parallel()
	f, p := newHostedBillingFixture(t)
	requireNativeStatus(t, postBillingEvent(t, f, "evt_unsigned", "invoice.paid", false), http.StatusBadRequest)
	var group sync.WaitGroup
	for range 8 {
		group.Go(func() {
			requireNativeStatus(t, postBillingEvent(t, f, "evt_paid", "invoice.paid", true), http.StatusOK)
		})
	}
	group.Wait()
	var count int
	if err := f.service.database.db.QueryRowContext(t.Context(), "SELECT count(*) FROM hosted_billing_events").Scan(&count); err != nil || count != 1 {
		t.Fatalf("events=%d %v", count, err)
	}
	requireNativeStatus(t, f.page(t, "owner", "/organization/billing?checkout=success&customer=cus_other"), http.StatusOK)
	entitlement, err := f.service.database.hostedPlanUsage(t.Context(), time.Now())
	if err != nil || entitlement.Source != "base" {
		t.Fatal("browser or event granted access")
	}
	p.snapshot = activeBillingSnapshot(time.Now())
	if err := f.service.billing.reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	first, err := f.service.database.readHostedBilling(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	requireNativeStatus(t, postBillingEvent(t, f, "evt_old_deleted", "customer.subscription.deleted", true), http.StatusOK)
	if err := f.service.billing.reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	current, err := f.service.database.readHostedBilling(t.Context())
	if err != nil || current != first {
		t.Fatalf("out of order event changed authoritative state: %+v %v", current, err)
	}
	if err := f.service.database.db.QueryRowContext(t.Context(), "SELECT count(*) FROM hosted_billing_audit WHERE action='subscription_reconciled'").Scan(&count); err != nil || count != 1 {
		t.Fatalf("duplicate audit=%d %v", count, err)
	}
	p.during = func() {
		requireNativeStatus(t, postBillingEvent(t, f, "evt_concurrent", "invoice.paid", true), http.StatusOK)
	}
	if err := f.service.billing.reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	p.during = nil
	if err := f.service.database.db.QueryRowContext(t.Context(), "SELECT count(*) FROM hosted_billing_events WHERE processed_at IS NULL").Scan(&count); err != nil || count != 1 {
		t.Fatalf("concurrent event was prematurely acknowledged: %d %v", count, err)
	}
	p.fail = true
	if err := f.service.billing.reconcile(t.Context()); err == nil {
		t.Fatal("outage succeeded")
	}
	calls := p.calls
	for _, at := range []time.Time{first.PaidThrough, first.AccessUntil.Add(-time.Nanosecond), first.AccessUntil} {
		entitlement, err := f.service.database.hostedPlanUsage(t.Context(), at)
		if err != nil {
			t.Fatal(err)
		}
		if (entitlement.Source == "subscription") != at.Before(first.AccessUntil) {
			t.Fatalf("deadline source=%s at %s", entitlement.Source, at)
		}
	}
	if p.calls != calls {
		t.Fatal("local entitlements called Stripe")
	}
	if err := f.service.Close(); err != nil {
		t.Fatal(err)
	}
	store, err := openDatabase(t.Context(), f.service.config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	if err := store.db.QueryRowContext(t.Context(), "SELECT count(*) FROM hosted_billing_events WHERE processed_at IS NULL").Scan(&count); err != nil || count != 1 {
		t.Fatalf("pending event lost after reopen=%d %v", count, err)
	}
	p.fail = false
	p.snapshot = billing.Snapshot{Status: "canceled"}
	worker := &hostedBillingWorker{service: &Service{database: store, config: f.service.config}}
	if err := worker.reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	entitlement, err = store.hostedPlanUsage(t.Context(), time.Now())
	if err != nil || entitlement.Source != "base" {
		t.Fatalf("recovery=%+v %v", entitlement, err)
	}
	if err := store.db.QueryRowContext(t.Context(), "SELECT count(*) FROM hosted_billing_events WHERE processed_at IS NULL").Scan(&count); err != nil || count != 0 {
		t.Fatalf("recovered event pending=%d %v", count, err)
	}
}

func TestHostedBillingAuthorizationAndCheckout(t *testing.T) {
	t.Parallel()
	f, p := newHostedBillingFixture(t)
	for _, account := range []string{"owner", "viewer", "staff", "support-viewer", "wrong-organization", "revoked", "expired", "missing"} {
		t.Run(account, func(t *testing.T) {
			calls, checkouts, portals := p.calls, len(p.checkouts), len(p.portals)
			for _, path := range []string{"/organization/billing", "/api/cloud/billing/subscription"} {
				response := f.page(t, account, path)
				if (response.Code == http.StatusOK) != (account == "owner") {
					t.Fatalf("%s response=%d", path, response.Code)
				}
			}
			if account == "missing" {
				return
			}
			for _, path := range []string{"/organization/billing/checkout", "/organization/billing/portal"} {
				response := f.form(t, account, path, url.Values{"price": {"price_fixture"}, "customer": {"cus_attacker"}, "organization": {"org_attacker"}})
				if (response.Code == http.StatusSeeOther) != (account == "owner") {
					t.Fatalf("%s response=%d %s", path, response.Code, response.Body.String())
				}
			}
			if account != "owner" && (p.calls != calls || len(p.checkouts) != checkouts || len(p.portals) != portals) {
				t.Fatal("unauthorized action reached Stripe")
			}
			if account == "owner" && (p.checkouts[0].CustomerID != "cus_fixture" || p.portals[0].CustomerID != "cus_fixture") {
				t.Fatal("client supplied customer was trusted")
			}
		})
	}
	f, p = newHostedBillingFixture(t)
	p.checkoutFail = true
	requireNativeStatus(t, f.form(t, "owner", "/organization/billing/checkout", url.Values{"price": {"price_fixture"}}), http.StatusServiceUnavailable)
	p.checkoutFail = false
	requireNativeStatus(t, f.form(t, "owner", "/organization/billing/checkout", url.Values{"price": {"price_fixture"}}), http.StatusSeeOther)
	if len(p.checkouts) != 2 || p.checkouts[0].IdempotencyKey != p.checkouts[1].IdempotencyKey || !p.checkouts[0].ExpiresAt.Equal(p.checkouts[1].ExpiresAt) {
		t.Fatal("uncertain checkout retry changed operation identity")
	}
	requireNativeStatus(t, f.form(t, "owner", "/organization/billing/checkout", url.Values{"price": {"price_fixture"}}), http.StatusSeeOther)
	if len(p.checkouts) != 2 {
		t.Fatal("duplicate checkout created a new session")
	}
	requireNativeStatus(t, f.form(t, "owner", "/organization/billing/checkout", url.Values{"price": {"price_attacker"}}), http.StatusBadRequest)
	r := httptest.NewRequest(http.MethodPost, "/organization/billing/checkout", strings.NewReader("price=price_fixture"))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	r.AddCookie(f.cookies["owner"])
	w := httptest.NewRecorder()
	f.service.Handler().ServeHTTP(w, r)
	requireNativeStatus(t, w, http.StatusForbidden)
	p.snapshot = activeBillingSnapshot(time.Now())
	requireNativeStatus(t, f.form(t, "owner", "/organization/billing/checkout", url.Values{"price": {"price_fixture"}}), http.StatusConflict)
}
