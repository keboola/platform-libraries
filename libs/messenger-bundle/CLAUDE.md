# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

`README.md` covers the configuration reference, DSN formats per platform, the Terraform-based local setup
and the procedure for reverse-engineering events off a real queue. Root `CLAUDE.md` has the monorepo
conventions.

## Commands

Docker service `dev-messenger-bundle` (PHP 8.2), which sets `APP_ENV=dev`.

```bash
docker compose run --rm dev-messenger-bundle composer ci      # validate + bootstrap + phpcs + phpstan + phpunit
docker compose run --rm dev-messenger-bundle vendor/bin/phpunit --filter testSerializer tests/ConnectionEvent/Serializer
```

Two things differ from every other library here:

- **PHPStan config is selected by `APP_ENV`**: `phpstan analyse -c phpstan-${APP_ENV}.neon`. The repo ships
  `phpstan-dev.neon`, `phpstan-test.neon` and `phpstan.neon.dist`; running `composer phpstan` without
  `APP_ENV` set fails with a missing-config error.
- `composer ci` runs `php tests/bootstrap.php` before the checks — it warms the test kernel; a stale
  `var/` cache is the usual cause of confusing DI failures.

Queue-consumption tests (`tests/ConnectionEvent/{AwsSqs,AzureServiceBus,GooglePubSub}ConsumptionTest.php`)
need real cloud queues provisioned via `provisioning/` (see README); the rest of the suite runs offline.

## Architecture

### Extension prepends rather than configures

`DependencyInjection\KeboolaMessengerExtension` does its real work in `prependExtension()`, injecting
transport configuration into `framework.messenger` **before** FrameworkBundle processes it — that is why
`platform` must resolve at container-build time and why env placeholders are explicitly resolved
(`resolveEnvPlaceholders(..., true)`) there. When `platform` is unset the extension returns early and
registers no transport at all, which is the supported "bundle installed but inactive" state.

### Platform is the only switch

The `Platform` enum (`aws` / `azure` / `gcp`) selects which serializer is bound to the connection-event and
audit-log transports: `Serializer\{AwsSqsSerializer, AzureServiceBusSerializer, GooglePubSubSerializer}`.
Each unwraps a different envelope shape (SNS/SQS body, Event Grid, Pub/Sub) and then hands the payload to
the same factories. Adding a platform means an enum case, a serializer, and a branch in the extension.

### Event factories, not message classes per event

`ConnectionEvent\AuditLog\AuditEventFactory` and
`ConnectionEvent\ApplicationEvent\ApplicationEventFactory` map an incoming event name to a typed event
class, falling back to `GenericAuditLogEvent` / `GenericApplicationEvent` when the name is unknown. Consumers
therefore always get an `EventInterface` and never fail on an unrecognized event — adding a typed class for
an event that previously fell through to the generic one is a behaviour change for handlers that match on
type.

### GPS transport decoration

`Transport\GpsTransportFactoryDecorator` wraps the third-party `gps://` transport factory to inject default
REST timeouts (`restOptions.timeout: 120`, `connect_timeout: 10`), because without them a consumer blocks
forever on a dead connection. Setting `client_config` explicitly in transport options or the DSN query
disables the defaults entirely — it is not merged.
