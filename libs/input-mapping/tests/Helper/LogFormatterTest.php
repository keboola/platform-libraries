<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Helper;

use Generator;
use Keboola\InputMapping\Helper\LogFormatter;
use PHPUnit\Framework\TestCase;

class LogFormatterTest extends TestCase
{
    /**
     * @dataProvider durationProvider
     */
    public function testFormatDuration(float $seconds, string $expected): void
    {
        self::assertSame($expected, LogFormatter::formatDuration($seconds));
    }

    public static function durationProvider(): Generator
    {
        yield 'zero' => [0.0, '0.00 s'];
        yield 'sub second' => [0.123456, '0.12 s'];
        yield 'rounds up' => [1.999, '2.00 s'];
        yield 'long stall' => [2070.123, '2070.12 s'];
    }

    /**
     * @dataProvider bytesProvider
     */
    public function testFormatBytes(int $bytes, string $expected): void
    {
        self::assertSame($expected, LogFormatter::formatBytes($bytes));
    }

    public static function bytesProvider(): Generator
    {
        yield 'zero' => [0, '0 B'];
        yield 'plain bytes' => [512, '512 B'];
        yield 'exactly one kilobyte' => [1024, '1.0 KB'];
        yield 'megabytes' => [126353408, '120.5 MB'];
        yield 'gigabytes' => [1932735283, '1.8 GB'];
    }

    /**
     * @dataProvider throughputProvider
     */
    public function testFormatThroughput(int $bytes, float $seconds, string $expected): void
    {
        self::assertSame($expected, LogFormatter::formatThroughput($bytes, $seconds));
    }

    public static function throughputProvider(): Generator
    {
        yield 'no duration' => ['bytes' => 1024, 'seconds' => 0.0, 'expected' => 'n/a'];
        yield 'negative duration' => ['bytes' => 1024, 'seconds' => -1.0, 'expected' => 'n/a'];
        yield 'megabytes per second' => ['bytes' => 10485760, 'seconds' => 2.0, 'expected' => '5.0 MB/s'];
        yield 'stalled transfer' => ['bytes' => 1024, 'seconds' => 10.0, 'expected' => '102 B/s'];
    }
}
