<?php

declare(strict_types=1);

namespace Keboola\Settle\Tests;

use Keboola\Settle\SettleFactory;
use Monolog\Handler\TestHandler;
use Monolog\Logger;
use PHPUnit\Framework\TestCase;
use RuntimeException;

class SettleFactoryTest extends TestCase
{
    public function testCreateSettle(): void
    {
        $logsHandler = new TestHandler();
        $logger = new Logger('tests', [$logsHandler]);
        $factory = new SettleFactory($logger);

        $settle = $factory->createSettle(2, 1);

        try {
            $settle->settle(
                fn($v) => $v === true,
                fn() => false,
            );

            self::fail('Settle is expected to fail');
        } catch (RuntimeException $e) {
            self::assertSame(
                'Failed to settle condition, actual value "false" does not match expectation',
                $e->getMessage(),
            );
        }

        $checks = array_filter(
            $logsHandler->getRecords(),
            fn(array $log) => $log['message'] === 'Checking current value',
        );

        $fails = array_filter(
            $logsHandler->getRecords(),
            fn(array $log) => $log['message'] === 'Current value does not match expectation',
        );

        // check logs contains 2 value checks and 2 compare failures
        self::assertCount(2, $checks);
        self::assertCount(2, $fails);
    }
}
