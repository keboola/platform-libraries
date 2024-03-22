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

#[ManageApiTokenAuth(scopes: ['something:manage'])]
#[StorageApiTokenAuth]
class Controller {
  public function __invoke(
    string $entityId,
    #[CurrentUser] TokenInterface $token,
  ) {
    // allows request with either valid X-KBC-ManageApiToken with 'something:manage' scope OR any valid X-StorageApi-Token
    // but with additional checks in controller
    $entity = $this->fetchEntity($entityId);
    if ($token instanceof StorageApiToken && $token->getProjectId() !== $entity->getProjectId()) {
      throw new AccessDeniedHttpException('...');
    }
  }
}
```

To use individual authentication attributes, you need to install appropriate client package:
* to use `StorageApiTokenAuth`, install `keboola/storage-api-client`
* to use `ManageApiTokenAuth`, install `keboola/kbc-manage-api-php-client`

> [!NOTE]
> If you forget to install appropriate client, you will get exception like
> `Service "Keboola\ApiBundle\Attribute\ManageApiTokenAuth" not found: the container inside "Symfony\Component\DependencyInjection\Argument\ServiceLocator" is a smaller service locator`

## License

MIT licensed, see [LICENSE](./LICENSE) file.
