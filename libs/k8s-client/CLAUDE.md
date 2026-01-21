# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Library Purpose

High-level Kubernetes client library built on top of `kubernetes/php-client`. Provides enhanced functionality including:
- Connection to multiple clusters
- Automatic result type handling (transparent `Status` vs actual response)
- Integrated retries for networking problems
- High-level operations (`createModels`, `deleteModels`, `waitWhileExists`, etc.)

## Development Commands

All commands must be run inside the Docker container:

```bash
# Enter the container
docker compose run --rm dev-k8s-client bash

# Inside container - install dependencies
composer install

# Run tests
composer tests          # PHPUnit
composer ci             # Full CI suite (validate + phpcs + phpstan + tests)

# Code quality
composer phpcs          # Check code style
composer phpcbf         # Fix code style automatically
composer phpstan        # Static analysis (level: max)
```

**Note:** This library uses PHP 8.2 and does NOT require environment variables for local development or unit tests.

### Running Individual Tests

```bash
# Run specific test file
docker compose run --rm dev-k8s-client bash -c "vendor/bin/phpunit tests/ApiClient/PodsApiClientFunctionalTest.php"

# Run specific test method
docker compose run --rm dev-k8s-client bash -c "vendor/bin/phpunit --filter testCreateAndDeletePod tests/ApiClient/PodsApiClientFunctionalTest.php"
```

## Architecture

### Three-Layer Structure

1. **ClientFacadeFactory** - Creates configured client instances
   - `GenericClientFacadeFactory` - For explicit cluster credentials
   - `InClusterClientFacadeFactory` - For Pods running inside K8s (uses service account)
   - `AutoDetectClientFacadeFactory` - Auto-detects environment
   - `EnvVariablesClientFacadeFactory` - Loads config from environment variables

2. **KubernetesApiClientFacade** - High-level facade providing:
   - Type-safe resource operations (`createModels`, `deleteModels`, `mergePatch`, etc.)
   - Convenience methods for multiple resources at once
   - Waiting operations (`waitWhileExists`)
   - Resource listing with pagination (`listMatching`)
   - Access to specific API clients via getters

3. **ApiClient Wrappers** - Namespace/cluster-scoped API wrappers
   - Wrap `kubernetes/php-client` API classes
   - Handle automatic result type detection (Status vs resource)
   - Integrated retry logic via `keboola/retry`
   - Base classes: `BaseNamespaceApiClient`, `BaseClusterApiClient`

### Supported Resources

**Namespace-scoped:**
- `ConfigMapsApiClient` - ConfigMaps
- `EventsApiClient` - Events
- `IngressesApiClient` - Ingresses
- `PersistentVolumeClaimsApiClient` - PVCs
- `PodsApiClient` - Pods (includes log streaming)
- `SecretsApiClient` - Secrets
- `ServicesApiClient` - Services
- `AppsApiClient` - Custom App CRD (Keboola-specific)
- `AppRunsApiClient` - Custom AppRun CRD (Keboola-specific)

**Cluster-scoped:**
- `PersistentVolumesApiClient` - PVs

### Custom Resources (CRDs)

The library includes custom Keboola CRDs for App and AppRun resources:
- Models: `src/Model/Io/Keboola/Apps/V1/`
- Used for billing and cost tracking
- CRD definitions must be installed on K8s clusters for functional tests (see README.md)

## Implementing New API Support

To add support for a new Kubernetes API:

1. **Create API client wrapper** in `src/ApiClient/`:
   - Extend `BaseNamespaceApiClient` (for namespaced resources) or `BaseClusterApiClient` (for cluster-scoped)
   - Wrap corresponding `kubernetes/php-client` API class
   - Handle result types automatically (most methods already handled by base class)

2. **Update `KubernetesApiClientFacade`**:
   - Inject new API client via constructor
   - Add getter method (e.g., `public function myResource(): MyResourceApiClient`)
   - Add resource class to `$resourceTypeClientMap` array
   - Update type annotations for generic methods (`createModels`, `deleteModels`, etc.)

3. **Update factories** in `ClientFacadeFactory/`:
   - `GenericClientFacadeFactory` - Instantiate new API client and inject into facade

## Code Quality Standards

### PHPStan Configuration
- Level: `max`
- Custom ignoreErrors for external library issues:
  - `src/BaseApi/*` - Guzzle and kubernetes-runtime return type mismatches
- Stub file: `tests/stubs/K8s.stub` for external type definitions

### PHP_CodeSniffer
Uses `keboola/coding-standard` with exclusions:
- Excludes type hint sniffs (library has some untyped parameters for compatibility)

## Testing

### Test Structure
- Unit tests: `tests/` - No K8s cluster required
- Functional tests: `tests/ApiClient/*FunctionalTest.php` - Require real K8s cluster

### Local Development Setup

Functional tests require a Kubernetes cluster. Use Terraform to provision local resources:

```bash
export NAME_PREFIX=your-name  # Make resources unique

cat <<EOF > ./provisioning/local/terraform.tfvars
name_prefix = "${NAME_PREFIX}"
EOF

terraform -chdir=./provisioning/local init -backend-config="key=k8s-client/${NAME_PREFIX}.tfstate"
terraform -chdir=./provisioning/local apply
./provisioning/local/update-env.sh azure  # or aws
```

This creates test clusters and generates environment variables for functional tests.

## Commit Message Format

Since this is part of a monorepo, use Conventional Commits with library name:

```
<type>(k8s-client): <description>
```

Examples:
- `feat(k8s-client): add support for DaemonSets API`
- `fix(k8s-client): handle timeout in waitWhileExists correctly`
- `refactor(k8s-client): extract retry configuration to factory`
