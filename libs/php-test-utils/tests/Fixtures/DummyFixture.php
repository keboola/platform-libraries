<?php

declare(strict_types=1);

namespace Keboola\PhpTestUtils\Tests\Fixtures;

use Keboola\PhpTestUtils\Fixtures\Dynamic\FixtureInterface;

class DummyFixture implements FixtureInterface
{
    public static int $initializeCalls = 0;
    public static int $cleanUpCalls = 0;

    public function initialize(): void
    {
        self::$initializeCalls++;
    }

    public function cleanUp(): void
    {
        self::$cleanUpCalls++;
    }
}
