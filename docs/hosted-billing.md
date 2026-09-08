# Hosted subscription billing

The shared product at `https://hub.detent.build` bills organizations only for use
of the operator's hosted service. Self-hosted Detent is free with any supported
authentication provider, including WorkOS/custom; auth selection never enables
Detent billing, a subscription check or Cloud networking.

The delivered Stripe implementation is an optional, operator-configured
**test-mode pilot** for an existing reserved Detent Cloud organization. The
subscription pays for Detent's hosted service. Customer model accounts, keys,
usage, and provider charges stay separate.
Free organizations need neither a card nor a Stripe customer. The shared-site
design requires local and self-hosted Hubs to reject hosted billing configuration and never initialize billing or contact Stripe. Current
self-hosting without `--hosted-config` already runs independently; separating that
flag's WorkOS/policy coupling is follow-up work, not a shipped auth-mode switch.

## Supported test-pilot configuration

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

## Delivered tenant purchase and administration

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

## Delivered tenant webhooks and recovery

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

## Shared-site customer and billing contract

The following design is for [#2343](https://github.com/digitaldrywood/detent/issues/2343),
after shared routing/provisioning #2341/#2342. Reuse current entitlement, grant,
owner authorization, journal, grace and reconciliation behavior. Static
`billing.customer_id` becomes a legacy import input, not per-customer operator
configuration. [Deployment examples](examples/hub/README.md) distinguish currently
supported fields from the proposed mode configuration.

On a verified, non-impersonating owner's first paid action, atomically persist a
customer-creation intent keyed by deployment, Stripe account, mode and immutable
organization ID. Serialize competing purchases. Send a stable provider idempotency
key and the expected organization metadata, then persist the returned customer ID
in a unique account/mode/customer-to-organization mapping. Recover uncertain
responses using that same operation and provider lookup; after a provider
idempotency window expires, reconcile before issuing another creation. Never
adopt a customer solely because user-supplied/provider metadata names an org.
Conflicting/missing bindings leave the operation pending repair and grant no paid
access. Free signup never creates a customer or needs a Stripe connection.

The registry retains only these billing references and operation status; the
tenant retains subscription/entitlement audit state. Use durable outbox/checkpoint
reconciliation across the two stores; no distributed transaction is assumed.
Checkout is enabled only after both agree on the binding. Customer creation does
not grant a paid plan. Store operation parameters before Checkout, reuse existing
preflight/retry rules and reconcile the authoritative subscription/invoice before
changing entitlements. Registry unavailability cannot route a purchase to another
customer; tenant dispatch keeps using bounded local entitlement state.

Owner actions live at `/organizations/ORG/billing/checkout` and `/portal` (POST).
All returns are server-generated from the configured shared origin and that ORG,
with only allowlisted result parameters on `/organizations/ORG/billing`. A return
never selects a customer or grants access. The UI shows pending verification,
free/complimentary/subscribed state, usage and contextual grace/limit messages in
the existing shell. Billing reads/portal/export remain available when over quota;
no global banner or project access is conferred by billing ownership.

### Environment separation and shared webhooks

The target operator setting is `deployment.mode: operator_hosted`, independent of
`auth.provider`. Billing is separately optional; when enabled `billing.mode`
defaults to `test`. An explicit `live` value requires matching live credentials,
objects and a separately authorized activation. A self-hosted deployment rejects
any billing block before provider initialization. Missing/invalid config fails
startup; neither auth choice nor a key prefix silently chooses a deployment mode.

| Binding | Test | Live target |
| --- | --- | --- |
| API key environment reference | `DETENT_STRIPE_TEST_KEY` (`sk_test_` / `rk_test_`) | `DETENT_STRIPE_LIVE_KEY` (`sk_live_` / `rk_live_`) |
| Webhook secret environment reference | `DETENT_STRIPE_TEST_WEBHOOK_SECRET` | `DETENT_STRIPE_LIVE_WEBHOOK_SECRET` |
| Shared endpoint | `https://hub.detent.build/webhooks/stripe/test` | `https://hub.detent.build/webhooks/stripe/live` |
| Object/event requirement | Verified account, mapped customer/price, `livemode=false` | Verified account, separate mapped customer/price, `livemode=true` |

Both secrets use `whsec_`; the prefix cannot establish mode. Verify the signature
with the endpoint's configured secret over raw bounded bytes before routing, then
validate mode and account against the durable mapping. Only the configured mode's
endpoint is active for that deployment; a separate test deployment remains
isolated when production activates live mode. Staging uses its own exact public
origin and account/environment configuration, not production session authority.
Stripe documents separate test/live keys and objects in [API keys](https://docs.stripe.com/keys).

The shared inbox stores only verified event ID/type, account/mode/customer
reference and delivery status before acknowledgment. Dispatch to the mapped tenant
with authenticated backend authority; duplicate/out-of-order delivery reuses the
existing idempotent reconciliation worker. A tenant outage leaves a durable pending
event. Unknown or deleted customers never cause tenant creation; record bounded
quarantine/tombstone disposition without content and never grant access. Forged
metadata, mismatched accounts/modes and invalid signatures fail before tenant
mutation. Limit body size, timestamp tolerance, queues and retries as in the pilot.
No provider payload is copied into the metadata registry or customer logs.

Customer, subscription, price, portal, operation and event IDs are namespaced by
operator deployment/account/mode. Mode changes cannot reinterpret persisted test
records as live. Activation requires a new validated live binding/configuration
revision and explicitly mapped live prices/plans; old test records remain isolated.
On rollback, disable new Checkout while preserving live audit/reconciliation
and cancellation access through the authorized live binding until obligations are
resolved. Resume testing only in the separate test deployment; changing the
production mode is not a billing rollback. Never delete paid records or reset grace to simulate rollback. Test and
live credentials must not coexist as automatic fallbacks.

Before live activation the operator must supply approved product/price mappings,
portal settings, business/tax/payment policy, retention and cancellation/refund
handling, secret distribution and webhook verification evidence. This issue
specifies the contract; it creates no Stripe accounts/prices/webhooks, enables no
live endpoint and authorizes no charge. Fixture tests cover live-shaped objects
and mismatch rejection without a live transaction. Paid activation is independent
of #2199's free pilot evidence gate.

Organization deletion blocks new purchases and reconciles/cancels the existing
subscription before retiring its routing reference; cancellation/erasure failures
remain resumable. Account deletion does not silently cancel other owners'
organizations. Retain minimal audited account/mode/customer tombstones under the
operator retention policy so delayed events cannot revive deleted access.

## Validation

Delivered integration tests use test-mode HTTP fixtures, never live Stripe keys
or charges. #2343 adds fixture-only live-mode shapes, customer-create crash/retry,
mode/account mismatch and two-organization routing cases from the
[shared-site trace](cloud-onboarding.md#shared-site-acceptance-trace).
Focused tests cover signatures and replay tolerance, customer/account
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
