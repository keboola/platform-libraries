<?php

declare(strict_types=1);

namespace Keboola\Settle\Tests\Comparator;

use Keboola\Settle\Comparator\IsSame;
use PHPUnit\Framework\TestCase;

class IsSameTest extends TestCase
{
    /**
     * @dataProvider provideInvokeTestData
     * @param mixed $targetValue
     * @param mixed $currentValue
     */
    public function testInvoke($targetValue, $currentValue, bool $expectedResult): void
    {
        self::assertSame($expectedResult, (new IsSame($targetValue))($currentValue));
    }

    public function provideInvokeTestData(): iterable
    {
        yield 'matching string' => [
            'target' => 'a',
            'current' => 'a',
            'result' => true,
        ];

        yield 'matching bool' => [
            'target' => false,
            'current' => false,
            'result' => true,
        ];

        yield 'matching array' => [
            'target' => ['a'],
            'current' => ['a'],
            'result' => true,
        ];

        yield 'not matching string' => [
            'target' => 'b',
            'current' => 'a',
            'result' => false,
        ];

        yield 'no matching array' => [
            'target' => ['a', 'c'],
            'current' => ['a'],
            'result' => false,
        ];

        yield 'not-strict match' => [
            'target' => 1,
            'current' => true,
            'result' => false,
        ];
    }
}
