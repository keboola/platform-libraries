<?php

declare(strict_types=1);

namespace Keboola\PhpTestUtils;

use ReflectionProperty;

trait AssertObjectPropertyValueTrait
{
    /**
     * We call PHPUnit's assertSame under the hood, so declare it abstract to be provided by TestCase.
     * @param mixed $expected
     * @param mixed $actual
     */
    abstract public static function assertSame($expected, $actual, string $message = ''): void;

    /**
     * Assert that an (even private) object property has the expected value.
     * The method is intended to be used inside PHPUnit TestCase via this trait,
     * similarly to built-in assertions (e.g., assertNotNull).
     */
    private static function assertObjectPropertyValue(mixed $expectedValue, object $client, string $property): void
    {
        $reflection = new ReflectionProperty($client, $property);
        $value = $reflection->getValue($client);
        self::assertSame($expectedValue, $value);
    }
}
