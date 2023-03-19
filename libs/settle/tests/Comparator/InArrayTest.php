<?php

declare(strict_types=1);

namespace Keboola\Settle\Tests\Comparator;

use Keboola\Settle\Comparator\InArray;
use PHPUnit\Framework\TestCase;

class InArrayTest extends TestCase
{
    /**
     * @dataProvider provideInvokeStrictTestData
     * @param mixed $currentValue
     */
    public function testInvokeStrict(array $targetValue, $currentValue, bool $expectedResult): void
    {
        self::assertSame($expectedResult, (new InArray($targetValue, true))($currentValue));
    }

    public function provideInvokeStrictTestData(): iterable
    {
        yield 'empty array' => [
            'target' => [],
            'current' => 'a',
            'result' => false,
        ];

        yield 'matching item' => [
            'target' => ['b', 'a'],
            'current' => 'a',
            'result' => true,
        ];

        yield 'no matching item' => [
            'target' => ['b', 'c'],
            'current' => 'a',
            'result' => false,
        ];

        yield 'strict match' => [
            'target' => [1],
            'current' => true,
            'result' => false,
        ];
    }

    /**
     * @dataProvider provideInvokeNotStrictTestData
     * @param mixed $currentValue
     */
    public function testInvokeNotStrict(array $targetValue, $currentValue, bool $expectedResult): void
    {
        self::assertSame($expectedResult, (new InArray($targetValue, false))($currentValue));
    }

    public function provideInvokeNotStrictTestData(): iterable
    {
        yield 'empty array' => [
            'target' => [],
            'current' => 'a',
            'result' => false,
        ];

        yield 'matching item' => [
            'target' => ['b', 'a'],
            'current' => 'a',
            'result' => true,
        ];

        yield 'no matching item' => [
            'target' => ['b', 'c'],
            'current' => 'a',
            'result' => false,
        ];

        yield 'strict match' => [
            'target' => [1],
            'current' => true,
            'result' => true,
        ];
    }
}
