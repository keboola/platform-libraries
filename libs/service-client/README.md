# Service Client
Service Client provides easy way to get Keboola services URLs. Usage:

```php

use Keboola\ServiceClient\ServiceDnsType;
use Keboola\ServiceClient\ServiceClient;

// by default configured to return public URLs
$serviceClient = new ServiceClient('eu-central-1.keboola.com');
$serviceClient->getStorageApiUrl(); // https://connection.eu-central-1.keboola.com
$serviceClient->getEncryptionUrl(); // https://encryption.eu-central-1.keboola.com

// explicitly request internal URL
$serviceClient->getStorageApiUrl(ServiceDnsType::INTERNAL); // http://connection-api.connection.scv.cluster.local

// can be configured to return internal URLs by default
$serviceClient = new ServiceClient('eu-central-1.keboola.com', ServiceDnsType::INTERNAL);
$serviceClient->getStorageApiUrl(); // http://connection-api.connection.scv.cluster.local
```

## License

MIT licensed, see [LICENSE](./LICENSE) file.
