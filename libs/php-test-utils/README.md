# PHP Test Utils

Utilities to make writing PHPUnit tests easier. Currently provides helpers for working with environment variables so that they're properly validated:

```php

use PHPUnit\Framework\TestCase;
use Keboola\PhpTestUtils\TestEnvVarsTrait;

final class MyEnvTest extends TestCase
{
    use TestEnvVarsTrait;

    public function testOptionalEnv(): void
    {
        $clientOptions = new ClientOptions(
            url: new ServiceClient(self::getRequiredEnv('HOSTNAME_SUFFIX'))->getConnectionServiceUrl(),
            token: self::getRequiredEnv('TEST_STORAGE_API_TOKEN_SNOWFLAKE'),
        );
}
```

- **getOptionalEnv(name)**: returns the env var value as a non-empty string, or `null` if not set/empty.
- **getRequiredEnv(name)**: returns the env var value as a non-empty string; fails the env if missing.
- **overrideEnv(name, value|null)**: sets or unsets the variable consistently in `$_ENV` and the process via `putenv`.

## License

MIT licensed, see [LICENSE](./LICENSE) file.

