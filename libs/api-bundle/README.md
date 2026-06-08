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
  storage_token_exchange:
    enabled: false                 # when true, #[StorageApiTokenAuth] also accepts kbc_at_*/kbc_pat_* tokens
    service_account_token_path: '/var/run/secrets/connection.keboola.com/serviceaccount/token'
    project_id_header: 'X-KBC-ProjectId'
    connection_dns_type: 'internal' # DNS type for internal resolver calls to Connection
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
authenticating with the service's own projected Kubernetes ServiceAccount JWT. The result is a
normal `StorageApiToken`, so controllers and `#[CurrentUser] StorageApiToken` keep working unchanged
— no controller change and no configuration switch.

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

## Testing controllers

`Keboola\ApiBundle\Test\AuthenticatorTestTrait` stubs the authenticators in functional
(`KernelTestCase`) tests so guarded controllers can be exercised without reaching real
Storage/Manage APIs:

```php
use Keboola\ApiBundle\Test\AuthenticatorTestTrait;

class MyActionTest extends KernelTestCase
{
    use AuthenticatorTestTrait;

    public function testIt(): void
    {
        // for #[StorageApiTokenAuth]
        $token = $this->setupFakeStorageApiToken(projectId: '123', features: ['my-feature']);

        // for #[ApplicationTokenAuth] — works for both the X-KBC-ManageApiToken
        // header and the Kubernetes ServiceAccount JWT
        $this->setupFakeManageApiToken('my-token', scopes: ['something:manage']);

        // for #[StorageApiTokenAuth] with a kbc_at_/kbc_pat_ programmatic token — stubs the
        // resolver client and Storage verification, so no Connection/Storage API call is made
        $token = $this->setupFakeConnectionToken(projectId: '123', features: ['my-feature']);
    }
}
```

## License

MIT licensed, see [LICENSE](./LICENSE) file.
