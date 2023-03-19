<?php

declare(strict_types=1);

namespace Keboola\Settle\Tests\Comparator;

use Keboola\Settle\Comparator\IsTrue;
use PHPUnit\Framework\TestCase;

class IsTrueTest extends TestCase
{
    /**
     * @dataProvider provideInvokeTestData
     * @param mixed $currentValue
     */
    public function testInvoke($currentValue, bool $expectedResult): void
    {
        self::assertSame($expectedResult, (new IsTrue())($currentValue)); // @phpstan-ignore-line
    }

    public function provideInvokeTestData(): iterable
    {
        yield 'true' => [
            'current' => true,
            'result' => true,
        ];

        yield 'false' => [
            'current' => false,
            'result' => false,
        ];

        yield 'positive integer' => [
            'current' => 1,
            'result' => false,
        ];

        yield 'empty array' => [
            'current' => [],
            'result' => false,
        ];

        yield 'non-empty array' => [
            'current' => ['a'],
            'result' => false,
        ];
    }
}
