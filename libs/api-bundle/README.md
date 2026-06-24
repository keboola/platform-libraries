# Keboola API Bundle
Symfony bundle providing common functionality for Keboola API applications.

## Installation
Install the package with Composer:
```shell
composer require keboola/api-bundle
```

## Configuration
The bundle expects having `%app_name%` parameter defined in your Symfony configuration.

### Default configuration
```yaml
keboola_api:
  app_name: '%app_name%'           # application name to use in user agent
  default_service_dns_type: public # default service DNS type to use in ServiceClient, can be 'public' or 'private'
```

## Features
### Preconfigured ServiceClient
The bundle provides a preconfigured `ServiceClient` that can be used to resolve Keboola API URLs. By default, it is
configured to use public hostnames, but it can be reconfigured to use internal ones.

```yaml
keboola_api:
  default_service_dns_type: internal
```

#### Using ENV variables

If you need to use ENV variable to configure the `default_service_dns_type`, make sure you provide some default value,
otherwise the validation will fail with error `The value "" is not allowed for path "keboola_api.default_service_dns_type".`

```yaml
parameters:
  env(API_DNS_TYPE): internal

keboola_api:
  default_service_dns_type: '%env(API_DNS_TYPE)%'
```

### Controller authentication using attributes
To use authentication using attributes, configure firewall to use the `keboola.api_bundle.security.attribute_authenticator`:
```yaml 
security:
  firewalls:
    attribute:
        lazy: true
        stateless: true
        custom_authenticators:
          - keboola.api_bundle.security.attribute_authenticator
```

Then add any combination of authentication attributes to your controller:
```php
use Keboola\ApiBundle\Attribute\StorageApiTokenAuth;
use Keboola\ApiBundle\Security\StorageApiToken\SecurityApiToken;
use Symfony\Component\Security\Http\Attribute\CurrentUser;

#[StorageApiTokenAuth]
class Controller {
  public function __invoke(#[CurrentUser] StorageApiToken $token) 
  {
    // only requests with valid X-StorageApi-Token will be allowed
  }
}

#[StorageApiTokenAuth(features: ['feat-a', 'feat-b'])]
class Controller {
  public function __invoke(#[CurrentUser] StorageApiToken $token) 
  {
    // only requests with valid X-StorageApi-Token and project features 'feat-a' AND 'feat-b' is allowed
  }
}

#[StorageApiTokenAuth(features: ['feat-a'])]
#[StorageApiTokenAuth(features: ['feat-b'])]
class Controller {
  public function __invoke(#[CurrentUser] StorageApiToken $token) 
  {
    // only requests with valid X-StorageApi-Token and any of project features 'feat-a' OR 'feat-b' ise allowed
  }
}

#[ApplicationTokenAuth(scopes: ['something:manage'])]
#[StorageApiTokenAuth]
class Controller {
  public function __invoke(
    string $entityId,
    #[CurrentUser] TokenInterface $token,
  ) {
    // allows request with a valid Manage API token (`X-KBC-ManageApiToken`) or a Kubernetes
    // ServiceAccount JWT (`X-Kubernetes-Authorization: Bearer <jwt>`) with the 'something:manage'
    // scope, OR any valid X-StorageApi-Token — but with additional checks in controller
    $entity = $this->fetchEntity($entityId);
    if ($token instanceof StorageApiToken && $token->getProjectId() !== $entity->getProjectId()) {
      throw new AccessDeniedHttpException('...');
    }
  }
}
```

`ApplicationTokenAuth` accepts both the Manage API token header (`X-KBC-ManageApiToken`)
and the Kubernetes ServiceAccount JWT header (`X-Kubernetes-Authorization`); Connection resolves
both to a Manage token, so `scopes`/`isSuperAdmin` checks are identical regardless of which header
the request carries.

### Connection programmatic tokens (Storage token exchange)

`#[StorageApiTokenAuth]` transparently accepts the new Connection programmatic bearer tokens
(`kbc_at_*` access tokens and `kbc_pat_*` personal access tokens) in addition to the legacy
`X-StorageApi-Token`. Programmatic tokens are exchanged for a legacy Storage token via the Manage
API client's `Client::resolveStorageToken()` (which calls Connection's internal resolver endpoint),
authenticating with the service's own projected Kubernetes ServiceAccount JWT. The resolver returns
the legacy token together with its full token detail (the same payload as Storage's
`tokens/verify`), so the exchange is a single HTTP call — no follow-up Storage verification. The
result is a normal `StorageApiToken`, so controllers and `#[CurrentUser] StorageApiToken` keep
working unchanged — no controller change and no configuration switch.

Callers send `Authorization: Bearer kbc_at_…`/`kbc_pat_…` together with an `X-KBC-ProjectId` header
(the new tokens are not project-scoped on their own).

```php
#[StorageApiTokenAuth]
class MyController {
  public function __invoke(#[CurrentUser] StorageApiToken $token) {
    // accepts X-StorageApi-Token (legacy) OR Authorization: Bearer kbc_at_/kbc_pat_ (+ X-KBC-ProjectId)
  }
}
```

This requires the service's ServiceAccount to be mapped to the
`internal:auth-bridge:resolve-storage-token` scope in Connection's Kubernetes-auth config. See
[docs/storage-token-exchange.md](docs/storage-token-exchange.md) for the full design, the resolver
contract, error mapping, and infrastructure prerequisites.

To use individual authentication attributes, you need to install appropriate client package:
* to use `StorageApiTokenAuth`, install `keboola/storage-api-client`

`ApplicationTokenAuth` and the Storage token exchange rely on `keboola/kbc-manage-api-php-client`,
which the bundle requires directly, so no extra installation is needed.

> [!NOTE]
> If you forget to install appropriate client, you will get exception like
> `Service "Keboola\ApiBundle\Attribute\ApplicationTokenAuth" not found: the container inside "Symfony\Component\DependencyInjection\Argument\ServiceLocator" is a smaller service locator`

## Storage API client

When `#[StorageApiTokenAuth]` is enabled, type-hint
`Keboola\ApiBundle\StorageApiClient\StorageClientApiFactory` on a controller argument; the bundle
injects a factory already bound to the current request and the resolved `StorageApiToken`. Unlike the
header-based `StorageClientRequestFactory`, it uses the token resolved by the authenticator, so it
works for programmatic (`kbc_pat_*` / `kbc_at_*`) tokens too.

The base options are preconfigured by the bundle: the Connection (Storage API) URL from the
`ServiceClient`, the shared logger, and the configured `app_name` as user agent. The run id comes
from the request's `X-KBC-RunId` header; branch / backend come from an optional per-call
`ClientOptions`.

```php
#[StorageApiTokenAuth]
public function __invoke(StorageClientApiFactory $storage)
{
    $client = $storage->createClientWrapper()->getBasicClient();

    // branch-aware / per-call overrides:
    // $wrapper = $storage->createClientWrapper(new ClientOptions(branchId: $branchId));
}
```

## Testing controllers

`Keboola\ApiBundle\Test\AuthenticatorTestTrait` stubs the authenticators in functional
(`WebTestCase`) tests so guarded controllers can be exercised without reaching real
Storage/Manage APIs. It provides four helpers:

| Helper | Stubs auth for | Returns |
| --- | --- | --- |
| `setupFakeStorageApiToken(tokenString?, projectId, features, adminId?)` | `#[StorageApiTokenAuth]` via legacy `X-StorageApi-Token` | `StorageApiToken` |
| `setupFakeConnectionToken(projectId, features, tokenString?, adminId?)` | `#[StorageApiTokenAuth]` via a `kbc_at_`/`kbc_pat_` programmatic token (stubs the exchange resolver client) | `StorageApiToken` |
| `setupFakeManageApiToken(tokenString, scopes, features)` | `#[ApplicationTokenAuth]` (both the `X-KBC-ManageApiToken` header and the Kubernetes ServiceAccount JWT) | `void` |
| `bootCleanClient()` | — boots a fresh, reboot-disabled `KernelBrowser` on a clean container | `KernelBrowser` |

### Why `bootCleanClient()`

The `setupFake*Token()` helpers replace services in the test container via
`getContainer()->set(...)`. That only works while those services are **not yet initialized**: a
`#[StorageApiTokenAuth]` request initializes `ManageApiClientFactory` (it backs the programmatic-token
exchange resolver), and an initialized service can no longer be replaced. So whenever a request has
already run in the test — or you simply want a guaranteed-clean container — call
`self::bootCleanClient()` first. It boots a fresh kernel, disables client reboot, and returns the
`KernelBrowser` to use for the request.

> [!IMPORTANT]
> `bootCleanClient()` **reboots the kernel**, discarding the previous container. Call it before
> **every** `getContainer()->set(...)` the test relies on — including your own app-specific service
> mocks, not just the `setupFake*Token()` helpers. Mocks registered *before* `bootCleanClient()` are
> thrown away by the reboot and will not be seen by the request.

Recommended order: **seed the database → `bootCleanClient()` → register service mocks →
`setupFake*Token()` → request.**

### Requirements

- The test case must extend Symfony's `WebTestCase` (`bootCleanClient()` uses `bootKernel()` /
  `getClient()` / the `test.client` service).
- `symfony/browser-kit` must be installed (dev dependency) for the `KernelBrowser` client.

### Example

```php
use Keboola\ApiBundle\Test\AuthenticatorTestTrait;
use Symfony\Bundle\FrameworkBundle\Test\WebTestCase;

class MyActionTest extends WebTestCase
{
    use AuthenticatorTestTrait;

    public function testUnauthorized(): void
    {
        // A request that doesn't fake a token can use the standard client.
        $client = static::createClient();
        $client->request('GET', '/my-endpoint');

        self::assertResponseStatusCodeSame(401);
    }

    public function testAuthorized(): void
    {
        self::setupDatabase([$entity]);          // 1) seed state the controller reads

        $client = self::bootCleanClient();       // 2) clean container BEFORE any ->set()

        $myService = $this->createMock(MyService::class);
        self::getContainer()->set(MyService::class, $myService);   // 3) app service mocks

        $token = $this->setupFakeStorageApiToken( // 4) stub the authenticator
            projectId: '123',
            features: ['my-feature'],
        );

        $client->request('GET', '/my-endpoint', server: [   // 5) authenticated request
            'HTTP_X_STORAGEAPI_TOKEN' => $token->getTokenValue(),
        ]);

        self::assertResponseIsSuccessful();
    }
}
```

Swap `setupFakeStorageApiToken()` for `setupFakeConnectionToken()` (programmatic token) or
`setupFakeManageApiToken()` (`#[ApplicationTokenAuth]`) depending on the attribute under test; the
clean-client ordering is the same for all three.

## License

MIT licensed, see [LICENSE](./LICENSE) file.
