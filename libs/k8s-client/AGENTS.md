# AGENTS.md

Guidance for AI coding agents working on the `k8s-client` library.

`README.md` covers usage, the credential factories, CRD registration, Symfony wiring, the Terraform-based
local cluster setup and the steps for implementing a new API. Root `AGENTS.md` has the monorepo conventions
(Docker workflow, commit format, coding standards). This file only adds what neither covers.

## Contributing — this repository is a mirror; pull requests go to the monorepo

`k8s-client` is developed in the
**[keboola/platform-libraries](https://github.com/keboola/platform-libraries)**
monorepo, under `libs/k8s-client/`. It is published to the standalone
**[keboola/k8s-client](https://github.com/keboola/k8s-client)** repository only so that Composer can
install it — that repository is a **read-only mirror**. CI re-splits the monorepo subdirectory into
it on every green build and force-pushes the result, so any commit made there is overwritten and lost.

- **Open pull requests against `keboola/platform-libraries`, never against `keboola/k8s-client`.**
  A pull request on the mirror cannot be merged and will be closed.
- If the checkout you are in has no `libs/` directory at its root, you are in the mirror. Stop, clone
  `keboola/platform-libraries`, and make the change in `libs/k8s-client/` there.
- Commit messages are Conventional Commits scoped to the library: `fix(k8s-client): …`.
- A release is a `k8s-client/<version>` tag pushed in the monorepo; the mirror's tag is derived from it
  with the `k8s-client/` prefix stripped.
- Monorepo-wide conventions (Docker-based dev workflow, coding standards, CI layout) are in the monorepo's
  root `AGENTS.md`.

## Commands

Docker service `dev-k8s-client` (PHP 8.2); no environment variables for unit tests.

```bash
docker compose run --rm dev-k8s-client composer ci   # validate + phpcs + phpstan + tests
docker compose run --rm dev-k8s-client vendor/bin/phpunit --filter testCreateAndDeletePod tests/ApiClient/PodsApiClientFunctionalTest.php
```

`composer phpcs` scans `.` with `--ignore=vendor,cache,Kernel.php`, not `src tests`.

`*FunctionalTest.php` files under `tests/ApiClient/` need a real cluster provisioned via `provisioning/`
(see README); everything else runs offline.

## Architecture

Three layers, and the split inside the first one is the part that isn't obvious from the README:

1. **`ClientFactory\`** holds two different things. The `KubernetesApiClientFactory` implementations
   (`Static`, `InCluster`, `EnvVariables`, `AutoDetect`) resolve credentials and produce a single configured
   `KubernetesApiClient`. Alongside them, `ClientConfigurator` and `Token\{TokenInterface, StaticToken,
   InClusterToken}` are shared low-level helpers those factories use to configure the underlying
   `kubernetes/php-client` `Client` **singleton** — which is why multi-cluster support has to go through
   this indirection rather than instantiating the vendor client directly.
2. **`KubernetesApiClientFacade`** is built by its own static `create()` (there is no separate facade
   factory class) and holds `$resourceTypeClientMap`, keyed by model class. `client(string $modelClass)`
   resolves any registered type, and the generic methods (`createModels`, `deleteModels`, `mergePatch`, …)
   route through the same map — so consumer-supplied CRD clients passed via `$extraClients` work with the
   generic methods too, not just `client()`.
3. **`ApiClient\` wrappers** extend `BaseNamespaceApiClient` or `BaseClusterApiClient`, which absorb the
   `Status`-vs-resource result ambiguity of the vendor client and apply `keboola/retry` retries.

Currently wrapped: `ConfigMaps`, `Events`, `Ingresses`, `PersistentVolumeClaims`, `Pods` (including log
streaming via `BaseApi\PodWithLogStream`), `Secrets`, `Services` — all namespace-scoped — plus
`PersistentVolumes`, the only cluster-scoped one.

`Event::class` is deliberately excluded from the resource list the facade reports for generic operations;
events are read-only and must not be swept into `createModels` / `deleteModels`.

The library owns **no** Keboola CRD model classes. Consumers (e.g. sandboxes-service's `App`/`AppRun`)
implement their own model/BaseApi/typed-client and register them through `$extraClients`.

## Tooling specifics

- PHPStan runs at `level: max` with a library-wide `missingType.iterableValue` ignore and
  `tests/stubs/K8s.stub` supplying types the vendor package doesn't declare. New code should not need to
  extend that ignore.
- `phpcs.xml` excludes the Slevomat parameter/property/return type-hint sniffs, because the vendor client's
  signatures force untyped boundaries in the wrappers.
