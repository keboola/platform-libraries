# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

`README.md` documents the API and the discarded-certificate caveat. Root `CLAUDE.md` has the monorepo
conventions.

## Commands

Docker service `dev-key-generator` (PHP 8.2); requires only `ext-openssl`, no environment variables.

```bash
docker compose run --rm dev-key-generator composer ci   # validate + phpcs + phpstan + tests
docker compose run --rm dev-key-generator vendor/bin/phpunit tests/PemKeyCertificateGeneratorTest.php
```

## Notes

Keep `#[SensitiveParameter]` on any new key-bearing parameter or property — it is what keeps private keys
out of stack traces, and it is easy to drop when adding an overload.

`CertificateSigningRequest::createDefault()` hard-codes Keboola's company identity (including the company
registration number as `serialNumber`). It is only reachable through `createPemKeyCertificate()`, whose CSR
is discarded, so nothing currently depends on those values — but they are real company data, not
placeholders.

## Consumers

`staging-provider`'s `Workspace\SnowflakeKeypairGenerator` wraps this to produce Snowflake key-pair login
credentials (calling it with a `null` password), and `input-mapping` / `output-mapping` pull it in
transitively. It is depended on as `*@dev`, so a signature change breaks those at install time and CI will
run all of them on any change here.
