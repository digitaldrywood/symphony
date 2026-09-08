# Hosted subscription billing

Stripe billing is an optional, operator-configured **test-mode pilot** for an
existing Detent Cloud organization. The subscription pays for Detent's hosted
service. Customer model accounts, keys, usage, and provider charges stay separate.
Free organizations need neither a card nor a Stripe customer. Local and
self-hosted Hubs do not initialize billing or contact Stripe.

## Configuration

Add the following to the hosted identity YAML, alongside explicit
[versioned entitlement plans](hosted-allowances.md). The referenced paid plan
must already appear in `entitlements.plans` and must differ from its base plan.

```yaml
billing:
  account_id: acct_example
  customer_id: cus_example
  portal_configuration_id: bpc_example
  api_key_env: DETENT_STRIPE_TEST_KEY
  webhook_secret_env: DETENT_STRIPE_TEST_WEBHOOK_SECRET
  grace_seconds: 86400
  reconcile_seconds: 120
  prices:
    - price_id: price_example
      label: Extended pilot
      plan: {id: pilot_extended, version: 1}
```

The operator provisions a test customer in the named Stripe account and sets
its `metadata.detent_organization_id` to this Hub's `organization_id`. This
binding is persisted in the owning organization database and cannot silently
change on restart. Price-to-plan mappings are also immutable: a changed plan
requires a new version and a new Stripe price. Removing a price from the
configured allowlist makes subscriptions using it ineligible at reconciliation.
These rules prevent changing credentials or configuration from reassigning
customer billing records between organizations.

Secret values are read only from the named environment variables. The adapter
requires an `sk_test_` or `rk_test_` key and a `whsec_` signing secret. It pins
Stripe API version `2025-06-30.basil` and validates explicit test-mode responses.
There is no live-mode configuration switch in this delivery. No prices, taxes,
live charges, or public signup are created or enabled by this change. Launching
live billing requires a separate approved operator/product change.

This pilot supports one automatically collected, single-item, quantity-one
subscription per customer, using a recurring licensed per-unit price. Checkout
verifies that the selected price is active and in test mode. Configure the test
portal to expose invoices, payment updates, cancellation, and only the approved
subscription prices. Scheduled downgrades take effect when Stripe actually
changes the subscription price; immediate changes apply at reconciliation.
Checkout does not enable automatic tax, promotion-code entry, or trials.
Existing Stripe-authorized trials and invoice discounts are evaluated separately
from complimentary grants. A paid invoice reduced to zero by an authorized
discount can still support the configured plan.

The configured grace is zero to seven days; reconciliation runs every 60 to
3600 seconds and at startup. These are validation bounds, not public pricing or
service-level promises. A complete reconciliation has a 45-second timeout;
individual Stripe requests have a 15-second timeout. Unsupported subscription
shapes do not grant access. Responses requiring more than 100 subscriptions or
100 payments/disputes fail reconciliation without applying a partial result;
operators must resolve that unsupported account history before resuming billing.

## Purchase and administration

`GET /organization/billing` shows the effective plan, subscription status,
complimentary access, reconciliation time, and the 50 most recent subscription
audit entries. Owners can submit CSRF-protected forms to:

- `POST /organization/billing/checkout`: buy an approved plan through Stripe.
- `POST /organization/billing/portal`: manage payment details, invoices,
  subscription changes, and cancellation in Stripe's test portal.

Every action checks current organization membership and requires the owner role
without support impersonation. Ordinary admins, members, viewers, staff,
cross-organization sessions, and runner/API tokens cannot perform these actions.
Customer/account IDs and return URLs come from server configuration; submitted
customer or organization IDs never select the billing account. The provider
also verifies the account ID and customer metadata before billing operations.

Checkout first reconciles the customer and refuses a second nonterminated
subscription. It persists an operation key and a one-hour expiry before sending
the request to Stripe. Matching retries reuse the same parameters and
idempotency key, including after an uncertain HTTP response or Hub restart.
An open checkout for one plan prevents creating another until expiry. Before
creating a replacement, the Hub reconciles again to detect any completed
purchase. Stripe validates and displays the amount before the customer accepts.

The success return URL displays a pending-verification message. Visiting it
never changes an entitlement. Checkout completion itself is also insufficient:
the authoritative subscription and current invoice must qualify.

Owner-only `GET /api/cloud/billing/subscription` exports the local subscription
state, entitlement, reconciliation time, pending event count, and recent audit.
`GET /api/cloud/billing` continues to export hosted usage. These reads and portal
access do not depend on plan quotas. Full audit records remain durable in
`hosted_billing_audit`; portal access and billing reads/exports also use the
existing hosted identity audit trail. Card numbers, payment methods, webhook
payloads, provider error bodies, and invoice/dispute evidence are not stored.
Stripe session URLs remain private to the owning database/browser and are
excluded from audit records and logs.

## Signed webhooks and recovery

Configure an organization-specific test webhook destination at
`<public_url>/webhooks/stripe` using its own signing secret. Subscribe to
`customer.subscription.*`, `invoice.*`, `checkout.session.*`,
`charge.refunded`, `charge.dispute.*`, and `refund.*` events.

The endpoint verifies the exact request bytes with HMAC-SHA256 and a five-minute
timestamp tolerance before accepting any event. It rejects live-mode events
and bodies exceeding one MiB. Relevant event IDs and types are durably inserted
with a unique constraint before returning success. Duplicate deliveries are
acknowledged without another record. Event payloads are not retained or used
to grant plans. Other customers' event payloads cannot select an organization.

The separate billing worker reconciles the configured customer directly with
Stripe. All reconciliations for this Hub serialize, including Checkout's
preflight. Event ordering and event creation timestamps never overwrite a newer
subscription snapshot. Subscription access, audit changes, and acknowledgments
through the captured event sequence commit in one SQLite transaction. Events
received during the remote read remain pending for the next reconciliation.
Failed reads or failed transactions preserve pending events and the previous
bounded access deadline. Startup and periodic reconciliation recover missed
webhooks, crashes, duplicate delivery, and temporary Stripe outages.

## Access policy

Dispatch reads only local entitlements. It makes no Stripe request. The local
clock applies deadlines even if Stripe is unavailable or an event is delayed.

| Authoritative state | Subscription-derived access |
| --- | --- |
| No subscription | Base plan; no payment record required. |
| Initial incomplete payment | No paid access until Stripe confirms a paid invoice and active subscription. |
| Active with a paid current invoice | Approved plan through its period end, plus configured renewal-verification grace. |
| Successful renewal | A new paid period advances the local paid-through deadline. |
| Renewal payment failure or unpaid current invoice | Previously verified paid plan through its original paid-through deadline plus configured grace. Retries and repeated events cannot restart grace or upgrade an unpaid plan. |
| Grace deadline, unpaid, paused, or unsupported subscription | Base plan; new allocations must fit remaining allowances. |
| Verified Stripe trial | Approved trial plan through the trial end, with no payment grace. A trial is not a complimentary grant. |
| Scheduled cancellation | Access ends at Stripe's cancellation time/period end, without extending cancellation by grace. |
| Immediate cancellation or actual downgrade | Base/new approved plan at authoritative reconciliation. |
| Full refund of current invoice payments | Suspend subscription-derived access. Partial refunds preserve it. |
| Current invoice payment dispute | Suspend while unresolved or lost; a won/closed warning dispute recovers access if the subscription otherwise qualifies. |
| Recovery | Restore the currently approved plan only after authoritative verification. |

Refund/dispute checks follow the current subscription invoice's payments to
their payment intents, charges, and disputes, verifying each customer binding.
Refunds/disputes on unrelated or previous-period invoices do not independently
cancel a currently paid period under this pilot policy. Unsupported external
payment records or incomplete payment evidence leave reconciliation pending
instead of inferring payment success. A subscription with multiple current
items or multiple nonterminated subscriptions requires operator correction.

Complimentary grants retain their own scope, audit reason, expiry, and revocation.
They continue to augment the applicable base/paid plan after cancellation,
refund, dispute, or payment failure. Stripe never edits grants. With Stripe
configured, the separate entitlement administrator can manage base assignments
and complimentary grants but cannot overwrite subscription-derived access
through the old `subscription`/`end_subscription` commands.

All access reductions reuse the existing allowance boundary: they block new
excess allocations while allowing running leases to renew/release, safe
completion/checkpointing, existing artifact retention, reading, export, and
billing administration. They do not delete customer data or bypass repository,
runner, provider-budget, or permission controls. Omitting billing configuration
stops billing activity; previously verified local access still expires at its
stored deadline rather than being extended indefinitely.

## Validation

All integration tests use test-mode HTTP fixtures, never live Stripe keys or
charges. Focused tests cover signatures and replay tolerance, customer/account
authorization, Checkout/portal requests, duplicate/reordered/concurrent event
delivery, transaction rollback, restart recovery, grace boundaries, renewals,
refunds/disputes, grant preservation, safe lease completion, local-only dispatch,
configuration immutability, and browser-visible billing states.

Run `go test -race ./internal/billing ./internal/hubserver ./internal/cli`,
`make generate`, and the repository's `make check` gate before shipping.
Browser verification uses isolated test instances on ephemeral ports.

Stripe references: [customer portal](https://docs.stripe.com/customer-management),
[subscription webhooks](https://docs.stripe.com/billing/subscriptions/webhooks),
[signature verification](https://docs.stripe.com/webhooks), and
[invoice payments](https://docs.stripe.com/api/invoice-payment/list).
