# Git Service API Client

## Installation

```bash
composer require keboola/git-service-api-client
```

## Usage

```php
use Keboola\GitServiceApiClient\ApiClientConfiguration;
use Keboola\GitServiceApiClient\Auth\KeboolaServiceAccountAuth;
use Keboola\GitServiceApiClient\Auth\ManageApiTokenAuth;
use Keboola\GitServiceApiClient\GitServiceApiClient;

// Default: projected Kubernetes ServiceAccount token from
// /var/run/secrets/connection.keboola.com/serviceaccount/token, re-read on
// every request so kubelet-rotated tokens are picked up automatically.
$client = new GitServiceApiClient('https://git-service.example.com');

// Manage API token (legacy)
$client = new GitServiceApiClient(
    'https://git-service.example.com',
    new ApiClientConfiguration(auth: new ManageApiTokenAuth($manageApiToken)),
);

// SA token from a non-default mount path
$client = new GitServiceApiClient(
    'https://git-service.example.com',
    new ApiClientConfiguration(
        auth: new KeboolaServiceAccountAuth('/var/run/secrets/tokens/connection-token'),
    ),
);
```

`KeboolaServiceAccountAuth` sends the bearer token in
`X-Kubernetes-Authorization`; `ManageApiTokenAuth` sends the legacy
`X-KBC-ManageApiToken` header.

## License

MIT licensed, see [LICENSE](./LICENSE) file.
