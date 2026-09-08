# Hub deployment examples and shared-site target

[Caddyfile](Caddyfile) and [detent-hub.service](detent-hub.service) are currently
supported **customer-operated, free self-hosting** examples. They serve a single
Hub at the customer's chosen origin with scoped bearer auth. Follow the
[self-hosting runbook](../../hub-self-hosting.md) for installation, credentials,
backup and recovery. They require no Detent subscription or billing connectivity.
Changing their hostname to hub.detent.build does not create the shared product.

The operator-hosted product has one public site at `https://hub.detent.build`,
a shared identity/provisioning entry and metadata registry, and dedicated private
tenant Hub processes with separate databases. All use the Detent binary; the
entry role is additional work in #2341/#2342. The public reverse proxy forwards
to that entry service, never chooses a tenant with a URL rewrite. The entry uses
[authenticated private routing](../../cloud-hub-rfc.md#shared-site-control-and-tenant-storage)
and each tenant verifies its own immutable binding. This directory installs no
shared-site service and authorizes no deployment, DNS/account change or purchase.

## Configuration availability

| Surface | Supported today | Shared-site target, not yet implemented |
| --- | --- | --- |
| Customer Hub | `detent hub serve --database PATH --listen ADDRESS --github-disabled`; private `DETENT_HUB_ADMIN_TOKEN`, optional TLS/trusted-proxy flags | Remains free; deployment mode independent of customer-selected auth |
| Reserved WorkOS tenant | `--hosted-config PATH`; root `organization_id`, `workos_organization_id`, `bootstrap_subject`, `public_url`, `workos`, `directory`, `staff_emails`, `support_actors`, entitlement/billing fields | Compatibility importer preserves IDs; no manual tenant YAML/bootstrap user/public origin at signup |
| WorkOS fields | `client_id`, `api_key_env` (default `WORKOS_API_KEY`), optional `api_url`, `issuer_url` | Same explicit provider wiring under independent `auth`; one shared callback and invitation entry |
| Pilot entitlements | Existing `entitlements` plans/assignments and separate administrator environment reference; see [allowances](../../hosted-allowances.md) | Allocator assigns a configured versioned free plan; no auth-provider inference |
| Stripe | Optional `billing.account_id`, `customer_id`, `portal_configuration_id`, `api_key_env`, `webhook_secret_env`, `grace_seconds`, `reconcile_seconds`, `prices`; test keys only | Optional `billing.mode: test/live`; registry owns customer mappings; no root per-customer configuration |
| Shared entry/registry/allocator | No supported YAML fields or CLI entry role yet | Versioned site configuration below; private entry/registry ownership and finite admission required |

The current hosted YAML parser rejects unknown fields. Do not pass the following
proposed configuration to `--hosted-config`. #2341/#2342 must deliver a versioned
parser and documented CLI wiring for the entry role; #2343 adds billing mode.
Until then there is no command in this document that launches shared self-service.

## Proposed site YAML contract

This is a reviewable schema proposal, not supported runtime configuration. Paths
and the two-tenant admission limit illustrate an isolated fixture only; production
limits require #2308 measurements and operator settings. Memory/disk byte values
must be measured positive integers before the configuration is deployable; the
placeholder strings deliberately prevent treating this as a working deployment.

```yaml
schema: 1
deployment:
  mode: operator_hosted
public_url: https://hub.detent.build
auth:
  provider: workos
  workos:
    client_id: client_example
    api_key_env: WORKOS_API_KEY
    issuer_url: https://api.workos.com
registry:
  database: /var/lib/detent-site/registry.db
allocation:
  tenant_root: /var/lib/detent-tenants
  max_tenants: 2
  max_concurrent_provisions: 1
  max_organizations_per_identity: 1
  memory_reserve_bytes: REPLACE_WITH_MEASURED_INTEGER
  tenant_memory_budget_bytes: REPLACE_WITH_MEASURED_INTEGER
  disk_reserve_bytes: REPLACE_WITH_MEASURED_INTEGER
  tenant_disk_budget_bytes: REPLACE_WITH_MEASURED_INTEGER
  retry_limit: 5
  retry_deadline_seconds: 900
proxy_auth:
  issuer: detent-site-example
  signing_key_env: DETENT_SITE_SIGNING_KEY
  mtls_certificate_file: /etc/detent-site/entry.crt
  mtls_private_key_file: /etc/detent-site/entry.key
  tenant_ca_file: /etc/detent-site/tenant-ca.crt
free_plan:
  id: pilot_free
  version: 1
```

The named free plan must exist in the configured versioned entitlement catalog;
example IDs/quantities are not approved commercial allowances. Generated tenant
configuration includes immutable organization/provider IDs, private endpoint,
database path and allocation generation, with scoped verification keys and service
identity. It contains no browser bootstrap subject or per-tenant public origin.
The allocator owns path/endpoint selection; request data cannot override them.
No wildcard DNS, certificates or public tenant ports are required. Co-located
services can use separately permissioned Unix sockets after equivalent peer
identity verification; remote services require authenticated private transport.

Proposed configuration validation must reject unknown fields, conflicting legacy
and site settings, a missing/non-HTTPS public origin outside loopback fixtures,
non-positive/unmeasured limits, untrusted paths/endpoints and mode/key mismatches.
`deployment.mode` defaults to `self_hosted` when omitted in the new schema;
`auth.provider` never changes it. `operator_hosted` explicitly enables hosted
resource policy; billing remains opt-in. Self-hosted mode rejects allocation,
registry and billing blocks before any Cloud/Stripe adapter starts. Authentication
adapters use their own validated issuer/client/secret configuration. WorkOS uses
the fields above; custom adapters must implement the supported auth interface,
not trust arbitrary proxy identity headers. Generic/local configuration remains
in its existing deployment surface; this proposal does not invent a custom-auth
protocol or imply that `--hosted-config` already separates those modes.

There is no automatic environment-to-YAML interpolation or implicit `.envrc`
loading. Environment references resolve only in the process that consumes them.
Missing named secrets fail startup without logging their values. In systemd use
a reviewed private `EnvironmentFile` or credential facility for the entry/billing
service; a developer's Mac shell does not populate a Linux service environment.
Tenant services receive only their scoped credentials, not the operator's WorkOS
or Stripe key. Never place secrets in the registry, command arguments or reports.

## WorkOS and Stripe wiring

The common WorkOS callback is exactly
`https://hub.detent.build/auth/oidc/callback`, with User invitation URL
`https://hub.detent.build/invite`. Every organization uses these values. Configure
client ID, API key and issuer from the same provider environment. See
[identity setup](../../hosted-identity.md#workos-application-setup) for invitation
verification and staging separation. Existing-origin migration must use the
[controlled procedure](../../hosted-identity.md#existing-origin-migration).

The target optional billing block keeps the pilot account/portal/price/plan fields
but replaces static `customer_id` with durable organization mappings and adds
`mode`. For example, append this **proposed** test block only with a matching paid
plan catalog and independently configured test account/portal/price objects:

```yaml
billing:
  mode: test
  account_id: acct_example
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

`test` is the billing default, never inferred from auth. Its exact webhook is
`https://hub.detent.build/webhooks/stripe/test`. Explicit separately authorized
`live` activation instead uses `DETENT_STRIPE_LIVE_KEY`,
`DETENT_STRIPE_LIVE_WEBHOOK_SECRET`, matching live account/price/customer/portal
bindings and `https://hub.detent.build/webhooks/stripe/live`. Each environment's
webhook secret, event livemode and account must agree; there is no fallback
between environments. Staging substitutes its separately configured origin.
See [billing lifecycle and rollback](../../hosted-billing.md#environment-separation-and-shared-webhooks).
Free signup omits billing and creates no Stripe customer or card requirement.

Before any deployment, the implementation must publish its actual CLI/schema
reference and validate this contract with synthetic two-tenant/provider fixtures,
capacity refusal and restore evidence. The design review does not install units,
change DNS/TLS, register WorkOS/Stripe endpoints, or activate live charging.
