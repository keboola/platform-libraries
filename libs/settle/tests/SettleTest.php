<?php

declare(strict_types=1);

namespace Keboola\Settle\Tests;

use Keboola\Settle\Settle;
use Keboola\Settle\SettleFactory;
use Monolog\Handler\TestHandler;
use Monolog\Logger;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use RuntimeException;

class SettleTest extends TestCase
{
    private static TestHandler $logsHandler;
    private LoggerInterface $logger;

    protected function setUp(): void
    {
        parent::setUp();

        self::$logsHandler = new TestHandler();
        $this->logger = new Logger('tests', [self::$logsHandler]);
    }

    public function testSettleImmediately(): void
    {
        $logger = new NullLogger();
        $factory = new SettleFactory($logger);

        $settle = new Settle($this->logger, 1, 1);
        $result = $settle->settle(
            fn($v) => $v === 1,
            fn() => 1,
        );

        self::assertSame(1, $result);
        self::assertLogsCount(1, 'Checking current value');
        self::assertLogsCount(1, 'Condition settled');
        self::assertLogsCount(0, 'Current value does not match expectation');
    }

    public function testSettleAfterFewTries(): void
    {
        $settle = new Settle($this->logger, 5, 1);

        $currentValue = function (): int {
            static $i = 0;
            return [1, 1, 1, 2][$i++];
        };

        $result = $settle->settle(
            fn($v) => $v === 2,
            $currentValue,
        );

        self::assertSame(2, $result);
        self::assertLogsCount(4, 'Checking current value');
        self::assertTrue(self::$logsHandler->hasRecordThatPasses(
            fn ($v) => $v['message'] === 'Checking current value' &&
                $v['context']['attempt'] === 1,
            Logger::DEBUG,
        ));
        self::assertLogsCount(1, 'Condition settled');
        self::assertTrue(self::$logsHandler->hasRecordThatPasses(
            fn ($v) => $v['message'] === 'Condition settled' &&
                $v['context']['currentValue'] === '2' &&
                $v['context']['attempts'] === 4,
            Logger::DEBUG,
        ));
        self::assertLogsCount(3, 'Current value does not match expectation');
        self::assertTrue(self::$logsHandler->hasRecordThatPasses(
            fn ($v) => $v['message'] === 'Current value does not match expectation' &&
                $v['context']['currentValue'] === '1' &&
                $v['context']['attempts'] === 1,
            Logger::DEBUG,
        ));
    }

    public function testSettleNever(): void
    {
        $settle = new Settle($this->logger, 3, 1);

        try {
            $settle->settle(
                fn($v) => $v === 2,
                fn() => 1,
            );
            self::fail('Settle was expected to fail');
        } catch (RuntimeException $e) {
            self::assertSame(
                'Failed to settle condition, actual value "1" does not match expectation',
                $e->getMessage(),
            );
        }

        self::assertLogsCount(3, 'Checking current value');
        self::assertLogsCount(0, 'Condition settled');
        self::assertLogsCount(3, 'Current value does not match expectation');
    }

    public function testSettleDoesDelay(): void
    {
        // should wait 0 + 2 + 4 + 8 = 14 sec
        $settle = new Settle($this->logger, 4, 60);

        $currentValue = function (): int {
            static $i = 0;
            return [1, 1, 1, 2][$i++];
        };

        $startTime = microtime(true);

        $settle->settle(
            fn($v) => $v === 2,
            $currentValue,
        );

        $endTime = microtime(true);
        $runTime = $endTime - $startTime;

        self::assertEqualsWithDelta(14, $runTime, 1);
    }

    public function testSettleDelayCapping(): void
    {
        // should wait 0 + 2 + 2 + 2 = 6 sec
        $settle = new Settle($this->logger, 4, 2);

        $currentValue = function (): int {
            static $i = 0;
            return [1, 1, 1, 2][$i++];
        };

        $startTime = microtime(true);

        $settle->settle(
            fn($v) => $v === 2,
            $currentValue,
        );

        $endTime = microtime(true);
        $runTime = $endTime - $startTime;

        self::assertEqualsWithDelta(6, $runTime, 1);
    }

    private static function assertLogsCount(int $expectedCount, string $message): void
    {
        self::assertCount(
            $expectedCount,
            array_filter(self::$logsHandler->getRecords(), fn(array $log) => $log['message'] === $message),
        );
    }
}
