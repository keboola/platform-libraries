# Keboola Logging Bundle
Features:
* adds DataDog APM metadata to logs

## Installation
Install the package with Composer:
```shell
composer require keboola/logging-bundle
```

To use authentication using attributes, add the following to your `config/packages/security.yaml`:
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
No configuration is required.

Based on if the DataDog extension is available, the bundle will automatically register the log processor.

## License

MIT licensed, see [LICENSE](./LICENSE) file.
