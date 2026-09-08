package hubserver

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/digitaldrywood/detent/internal/billing"
	"github.com/digitaldrywood/detent/internal/tracker"
)

func TestHostedBillingDowngradePreservesGrantsAndData(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name, hold string
		grant      bool
	}{
		{"canceled", "", false}, {"refunded", "refunded", false}, {"disputed", "disputed", false}, {"complimentary", "", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			f, p := newHostedBillingFixture(t)
			p.snapshot = activeBillingSnapshot(time.Now())
			if err := f.service.billing.reconcile(t.Context()); err != nil {
				t.Fatal(err)
			}
			project := f.createProject(t, "Paid project")
			if test.grant {
				entitlement, err := f.service.database.hostedPlanUsage(t.Context(), time.Now())
				if err != nil {
					t.Fatal(err)
				}
				if err := f.service.database.applyHostedPlanCommand(t.Context(), "operator_fixture", hostedPlanCommand{ID: "grant_fixture", GrantID: "complimentary_fixture", Action: "grant", ExpectedRevision: entitlement.Revision, Plan: PlanReference{ID: "test_paid", Version: 1}, Scope: []string{"projects"}, Reason: "Approved complimentary pilot access"}); err != nil {
					t.Fatal(err)
				}
			}
			if test.hold != "" {
				p.snapshot.PaymentHold = test.hold
			} else {
				p.snapshot = billing.Snapshot{Status: "canceled"}
			}
			if err := f.service.billing.reconcile(t.Context()); err != nil {
				t.Fatal(err)
			}
			response := f.form(t, "owner", "/projects", url.Values{"name": {"New excess project"}, "grant_access": {"true"}})
			want := http.StatusTooManyRequests
			if test.grant {
				want = http.StatusSeeOther
			}
			requireNativeStatus(t, response, want)
			for _, path := range []string{"/projects/" + project, "/organization/plan", "/organization/billing", "/api/cloud/billing", "/api/cloud/billing/subscription"} {
				requireNativeStatus(t, f.page(t, "owner", path), http.StatusOK)
			}
			page := f.page(t, "owner", "/organization/billing").Body.String()
			if test.grant && !strings.Contains(page, "Complimentary access") {
				t.Fatal("billing hid the independent grant")
			}
			var projects int
			if err := f.service.database.db.QueryRowContext(t.Context(), "SELECT count(*) FROM projects").Scan(&projects); err != nil || projects < 3 {
				t.Fatalf("downgrade removed project data: %d %v", projects, err)
			}
			entitlement, err := f.service.database.hostedPlanUsage(t.Context(), time.Now())
			if err != nil || entitlement.Source != "base" {
				t.Fatalf("fallback=%+v %v", entitlement, err)
			}
			if err := f.service.database.applyHostedPlanCommand(t.Context(), "operator_fixture", hostedPlanCommand{ID: "override", Action: "subscription", ExpectedRevision: entitlement.Revision, Reason: "Attempted bypass"}); err == nil {
				t.Fatal("manual subscription override bypassed Stripe")
			}
		})
	}
}

func TestHostedBillingPreservesRunningLease(t *testing.T) {
	t.Parallel()
	f := newHostedSecurityFixture(t)
	plans := hostedTestPlans(t, f.service, map[string]int64{"concurrent_work": 0})
	now := time.Now().UTC()
	f.service.config.Hosted.Plans = &plans
	cfg := &HostedBillingConfig{Prices: []HostedBillingPrice{{PriceID: "price_fixture", Plan: plans.Plans[1].PlanReference}}, AccountID: "acct_fixture", CustomerID: "cus_fixture"}
	f.service.config.Hosted.Billing = cfg
	if err := f.service.database.configureHostedBilling(t.Context(), f.service.config.Hosted); err != nil {
		t.Fatal(err)
	}
	paid := resolveHostedBilling(cfg, hostedBillingState{}, activeBillingSnapshot(now), now)
	if err := f.service.database.commitHostedBilling(t.Context(), paid, 0, now); err != nil {
		t.Fatal(err)
	}
	requests := make([]tracker.ClaimRequest, 2)
	for i := range 2 {
		item := f.seedIssue(t, i+1)
		var id tracker.WorkItemID
		if err := f.service.database.db.QueryRowContext(t.Context(), "SELECT id FROM issues WHERE native_id=?", item).Scan(&id); err != nil {
			t.Fatal(err)
		}
		machine := tracker.MachineID(fmt.Sprintf("billing_machine_%d", i))
		if _, err := f.service.database.db.ExecContext(t.Context(), `INSERT INTO machines(id,hostname,display_name,capacity,version,last_heartbeat_at,registered_at,updated_at,organization_id,token_id) VALUES(?,?,?,1,'test',?,?,?,'org_security','bootstrap-admin')`, machine, machine, machine, formatHubTime(now), formatHubTime(now), formatHubTime(now)); err != nil {
			t.Fatal(err)
		}
		requests[i] = tracker.ClaimRequest{WorkItemID: id, MachineID: machine, SessionID: fmt.Sprintf("billing_session_%d", i), TTL: time.Minute}
	}
	lease, err := f.service.Tracker().Claim(t.Context(), requests[0])
	if err != nil {
		t.Fatal(err)
	}
	canceled := resolveHostedBilling(cfg, paid, billing.Snapshot{Status: "canceled"}, now)
	if err := f.service.database.commitHostedBilling(t.Context(), canceled, 0, now); err != nil {
		t.Fatal(err)
	}
	if _, err := f.service.Tracker().Renew(t.Context(), tracker.RenewRequest{LeaseID: lease.ID, FencingToken: lease.FencingToken, TTL: time.Minute}); err != nil {
		t.Fatalf("billing cancellation blocked running work: %v", err)
	}
	if _, err := f.service.Tracker().Claim(t.Context(), requests[1]); err == nil {
		t.Fatal("new excess dispatch bypassed cancellation")
	}
	if err := f.service.Tracker().Release(t.Context(), tracker.ReleaseRequest{LeaseID: lease.ID, FencingToken: lease.FencingToken, Reason: "completed"}); err != nil {
		t.Fatalf("safe release failed: %v", err)
	}
}

func TestHostedBillingConfiguration(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name string
		edit func(*HostedBillingConfig)
	}{
		{"missing provider", func(c *HostedBillingConfig) { c.Provider = nil }},
		{"missing account", func(c *HostedBillingConfig) { c.AccountID = "" }},
		{"missing customer", func(c *HostedBillingConfig) { c.CustomerID = "" }},
		{"missing portal", func(c *HostedBillingConfig) { c.PortalConfigurationID = "" }},
		{"missing secret", func(c *HostedBillingConfig) { c.WebhookSecret = nil }},
		{"negative grace", func(c *HostedBillingConfig) { c.GraceSeconds = -1 }},
		{"excess grace", func(c *HostedBillingConfig) { c.GraceSeconds = 8 * 86400 }},
		{"unbounded polling", func(c *HostedBillingConfig) { c.ReconcileSeconds = 1 }},
		{"unknown plan", func(c *HostedBillingConfig) { c.Prices[0].Plan.ID = "unknown" }},
		{"base plan purchase", func(c *HostedBillingConfig) { c.Prices[0].Plan.ID = "test_free" }},
		{"duplicate price", func(c *HostedBillingConfig) { c.Prices = append(c.Prices, c.Prices[0]) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			f, _ := newHostedBillingFixture(t)
			config := *f.service.config.Hosted.Billing
			config.Prices = slices.Clone(config.Prices)
			test.edit(&config)
			if err := config.validate(f.service.config.Hosted.Plans); err == nil {
				t.Fatal("invalid billing configuration accepted")
			}
		})
	}
	f, _ := newHostedBillingFixture(t)
	config := *f.service.config.Hosted
	changed := *config.Billing
	config.Billing = &changed
	changed.CustomerID = "cus_other"
	if err := f.service.database.configureHostedBilling(t.Context(), &config); err == nil {
		t.Fatal("customer binding changed")
	}
	changed.CustomerID = "cus_fixture"
	changed.Prices = slices.Clone(changed.Prices)
	changed.Prices[0].Plan.ID = "test_free"
	if err := f.service.database.configureHostedBilling(t.Context(), &config); err == nil {
		t.Fatal("price was repointed to another plan")
	}
	if err := f.service.database.configureHostedBilling(t.Context(), f.service.config.Hosted); err != nil {
		t.Fatalf("same configuration could not reopen: %v", err)
	}
}

func TestHostedBillingCommitIsAtomic(t *testing.T) {
	t.Parallel()
	f, p := newHostedBillingFixture(t)
	requireNativeStatus(t, postBillingEvent(t, f, "evt_atomic", "invoice.paid", true), http.StatusOK)
	if _, err := f.service.database.db.ExecContext(t.Context(), `CREATE TRIGGER reject_billing_ack BEFORE UPDATE ON hosted_billing_events BEGIN SELECT RAISE(ABORT,'fixture acknowledgment failure'); END`); err != nil {
		t.Fatal(err)
	}
	p.snapshot = activeBillingSnapshot(time.Now())
	if err := f.service.billing.reconcile(t.Context()); err == nil {
		t.Fatal("failed acknowledgment committed")
	}
	state, err := f.service.database.readHostedBilling(t.Context())
	if err != nil || state.Status != "free" {
		t.Fatalf("state partially committed: %+v %v", state, err)
	}
	entitlement, err := f.service.database.hostedPlanUsage(t.Context(), time.Now())
	if err != nil || entitlement.Source != "base" {
		t.Fatal("entitlement partially committed")
	}
	var count int
	if err := f.service.database.db.QueryRowContext(t.Context(), "SELECT count(*) FROM hosted_billing_audit").Scan(&count); err != nil || count != 0 {
		t.Fatalf("audit partially committed: %d %v", count, err)
	}
}

type blockingHostedBillingProvider struct {
	hostedBillingProvider
	started chan struct{}
}

func (p *blockingHostedBillingProvider) Reconcile(ctx context.Context, _ billing.Binding) (billing.Snapshot, error) {
	close(p.started)
	<-ctx.Done()
	return billing.Snapshot{}, ctx.Err()
}

func TestHostedBillingWorkerShutdown(t *testing.T) {
	t.Parallel()
	f, _ := newHostedBillingFixture(t)
	if err := f.service.Close(); err != nil {
		t.Fatal(err)
	}
	p := &blockingHostedBillingProvider{started: make(chan struct{})}
	cfg := f.service.config
	cfg.Hosted.Billing.Provider = p
	service, err := Open(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { service.Close() })
	<-p.started
	if err := service.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-service.billing.done:
	default:
		t.Fatal("billing reconciliation outlived database shutdown")
	}
}

func TestHostedBillingPageStates(t *testing.T) {
	t.Parallel()
	for _, test := range []struct{ name, status, invoice, want string }{
		{"free", "free", "", "Free access requires no card"},
		{"subscribed", "active", "paid", "Subscribed"},
		{"canceled", "canceled", "", "Canceled"},
		{"failed", "past_due", "open", "Payment failed"},
		{"trial", "trialing", "", "Subscription trial"},
		{"paused", "paused", "", "Subscription needs attention"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			f, p := newHostedBillingFixture(t)
			snapshot := activeBillingSnapshot(time.Now())
			snapshot.Status, snapshot.InvoiceStatus = test.status, test.invoice
			if test.status == "free" || test.status == "canceled" {
				snapshot = billing.Snapshot{Status: test.status}
			}
			if test.status == "trialing" {
				snapshot.TrialEnd = snapshot.PeriodEnd
			}
			p.snapshot = snapshot
			if err := f.service.billing.reconcile(t.Context()); err != nil {
				t.Fatal(err)
			}
			calls := p.calls
			response := f.page(t, "owner", "/organization/billing")
			requireNativeStatus(t, response, http.StatusOK)
			for _, want := range []string{test.want, "model provider accounts", `aria-label="Billing audit trail"`, `name="csrf"`, "Export billing status"} {
				if !strings.Contains(response.Body.String(), want) {
					t.Errorf("billing page missing %q", want)
				}
			}
			if p.calls != calls {
				t.Fatal("page read called Stripe")
			}
			if strings.Contains(response.Body.String(), "card-sentinel") {
				t.Fatal("webhook private data reached the page")
			}
		})
	}
}

func TestHostedBillingRenewalAndPlanChange(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	config := &HostedBillingConfig{GraceSeconds: 3600, Prices: []HostedBillingPrice{
		{PriceID: "price_fixture", Plan: PlanReference{ID: "extended", Version: 1}},
		{PriceID: "price_lower", Plan: PlanReference{ID: "limited", Version: 1}},
	}}
	paid := resolveHostedBilling(config, hostedBillingState{}, activeBillingSnapshot(now), now)
	for _, test := range []struct {
		name, price, invoice string
		end                  time.Time
		wantPlan             string
		wantEnd              time.Time
	}{
		{"renewal", "price_fixture", "paid", now.Add(31 * 24 * time.Hour), "extended", now.Add(31*24*time.Hour + time.Hour)},
		{"paid downgrade", "price_lower", "paid", now.Add(31 * 24 * time.Hour), "limited", now.Add(31*24*time.Hour + time.Hour)},
		{"unpaid plan change", "price_lower", "open", now.Add(31 * 24 * time.Hour), "extended", paid.AccessUntil},
	} {
		t.Run(test.name, func(t *testing.T) {
			snapshot := activeBillingSnapshot(now)
			snapshot.PriceID, snapshot.InvoiceStatus, snapshot.PeriodEnd = test.price, test.invoice, test.end
			snapshot.InvoiceID = "in_renewal"
			state := resolveHostedBilling(config, paid, snapshot, now)
			if state.Plan.ID != test.wantPlan || !state.AccessUntil.Equal(test.wantEnd) {
				t.Fatalf("state=%+v", state)
			}
			if test.invoice == "open" {
				later := resolveHostedBilling(config, state, snapshot, now.Add(4*time.Hour))
				if !later.AccessUntil.Equal(state.AccessUntil) {
					t.Fatal("retry restarted grace")
				}
			}
		})
	}
}

func TestHostedBillingRejectsAdminAndSupportOwner(t *testing.T) {
	t.Parallel()
	for _, support := range []bool{false, true} {
		t.Run(strconv.FormatBool(support), func(t *testing.T) {
			t.Parallel()
			f, p := newHostedBillingFixture(t)
			if support {
				identity := f.provider.identity("user_browser_owner", "owner@example.test", "org_browser_provider", "support@example.test")
				token, _, err := f.service.hostedSessions.CreateIdentitySession(t.Context(), identity)
				if err != nil {
					t.Fatal(err)
				}
				f.cookies["owner"] = &http.Cookie{Name: hostedCookie, Value: token}
			} else {
				if err := f.provider.SetMembershipRole(t.Context(), "membership_user_browser_owner", "admin"); err != nil {
					t.Fatal(err)
				}
			}
			requireNativeStatus(t, f.page(t, "owner", "/organization/billing"), http.StatusForbidden)
			for _, path := range []string{"/organization/billing/checkout", "/organization/billing/portal"} {
				requireNativeStatus(t, f.form(t, "owner", path, url.Values{"price": {"price_fixture"}}), http.StatusForbidden)
			}
			if p.calls != 0 || len(p.checkouts) != 0 || len(p.portals) != 0 {
				t.Fatal("admin or support owner reached Stripe")
			}
		})
	}
}
