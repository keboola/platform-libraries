<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Helper;

use Keboola\InputMapping\Helper\Timer;
use PHPUnit\Framework\TestCase;

class TimerTest extends TestCase
{
    public function testMeasuresElapsedTime(): void
    {
        $timer = Timer::start();
        usleep(20000);
        $elapsed = $timer->getElapsedSeconds();

        self::assertGreaterThanOrEqual(0.015, $elapsed);
        self::assertLessThan(5.0, $elapsed);
    }

    public function testElapsedTimeIsNonDecreasing(): void
    {
        $timer = Timer::start();
        $first = $timer->getElapsedSeconds();
        usleep(5000);
        $second = $timer->getElapsedSeconds();

        self::assertGreaterThanOrEqual($first, $second);
    }
}
