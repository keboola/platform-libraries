# PHP Test Utils

Utilities to make writing PHPUnit tests easier. Currently provides helpers for working with environment variables so that they're properly validated:

## Usage
### TestEnvVarsTrait
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

### Array and Object Property Assertions

The library also provides small helpers for asserting nested array values and private object properties via traits:

- AssertArrayPropertySameTrait: assert that a dot-separated path inside an array equals the expected scalar value.
- AssertObjectPropertyValueTrait: assert that an object's property has the expected value.

```php
use PHPUnit\Framework\TestCase;
use Keboola\PhpTestUtils\AssertArrayPropertySameTrait;
use Keboola\PhpTestUtils\AssertObjectPropertyValueTrait;

final class MyAssertionsTest extends TestCase
{
    use AssertArrayPropertySameTrait;
    use AssertObjectPropertyValueTrait;

    public function testHelpers(): void
    {
        // Assert nested array property value
        $row = ['customer' => ['id' => 123, 'name' => 'Acme']];
        self::assertArrayPropertySame(123, $row, 'customer.id');

        // Assert (even private) object property value
        $obj = new class() {
            private string $token = 'abc';
        };
        self::assertObjectPropertyValue('abc', $obj, 'token');
    }
}
```


## Development

Clone this repository and init the workspace with following command:

```
git clone https://github.com/keboola/platform-libraries.git
cd php-test-utils
docker-compose build
docker-compose run --rm dev composer install --no-scripts
```

Create `.env.local` file with following contents:
Set the token to master storage token to a Snowflake project.

```shell
HOSTNAME_SUFFIX=keboola.com
TEST_STORAGE_API_TOKEN_SNOWFLAKE=xxx-xxxxx-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
```

Run the test suite using this command:

```
docker-compose run --rm dev composer tests
```

## License

MIT licensed, see [LICENSE](./LICENSE) file.
