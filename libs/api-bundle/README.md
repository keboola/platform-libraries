# Keboola API Bundle
Symfony bundle providing common functionality for Keboola API applications.

Features:
* authentication using Storage and Manage API tokens

## Installation
Install the package with Composer:
```shell
composer require keboola/api-bundle
```

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

## Configuration
The default configuration is:

```yaml
keboola_api:
  app_name: '%app_name%'             # application name to use in user agent
  default_service_dns_type: 'public' # default service DNS type to use in ServiceClient, can be 'public' or 'private'
```

Authentication attributes are configured automatically based on API clients installed:
* to use `StorageApiTokenAuth`, install `keboola/storage-api-client`
* to use `ManageApiTokenAuth`, install `keboola/kbc-manage-api-php-client`

> [!NOTE]
> If you forget to install appropriate client, you will get exception like
> `Service "Keboola\ApiBundle\Attribute\ManageApiTokenAuth" not found: the container inside "Symfony\Component\DependencyInjection\Argument\ServiceLocator" is a smaller service locator`

## License

MIT licensed, see [LICENSE](./LICENSE) file.
