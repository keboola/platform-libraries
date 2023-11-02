# Settle
Library which provides a function that waits for something to happen. Usage:

```php

use Keboola\Settle\SettleFactory;
use Psr\Log\NullLogger;

$logger = new NullLogger();
$factory = new SettleFactory($logger);

$settle = $factory->createSettle(maxAttempts: 10, maxAttemptsDelay: 1);
$i = 0;
$settle->settle(
    comparator: fn($v) => $v === 5,
    getCurrentValue: function() use (&$i) {while ($i < 5) {$i++; return $i;}},
);
```

The `settle` function expects two callbacks - `comparator` and `getCurrentValue`. In a typical scenario, the 
`getCurrentValue` checks and returns the result of some asynchronous operation (data loaded, process finished) and 
the `comparator` checks that the `getCurrentValue` result is the expected value. If the expected value is not 
reached within the specified number of attempts a `RuntimeException` is thrown.

## License

MIT licensed, see [LICENSE](./LICENSE) file.
