<?php

declare(strict_types=1);

namespace Keboola\PhpTestUtils;

use ArrayAccess;

trait AssertArrayPropertySameTrait
{
    /**
     * Declare PHPUnit assertions to be provided by TestCase.
     * @param mixed $expected
     * @param mixed $actual
     */
    abstract public static function assertSame($expected, $actual, string $message = ''): void;

    /**
     * @param mixed $actual
     */
    abstract public static function assertIsArray($actual, string $message = ''): void;

    /**
     * @param mixed $actual
     */
    abstract public static function assertIsScalar($actual, string $message = ''): void;

    /**
     * @param array<mixed>|ArrayAccess<array-key, mixed> $array
     */
    abstract public static function assertArrayHasKey(mixed $key, array|ArrayAccess $array, string $message = ''): void;

    /**
     * Assert that a nested (dot-separated) property in an array is the expected scalar value.
     * Example: assertArrayPropertySame($id, $row, 'customer.id')
     * The method is intended to be used inside PHPUnit TestCase via this trait,
     * similarly to built-in assertions (e.g., assertNotNull).
     *
     * @param int|string|float|bool|null $expectedValue
     * @param $array
     */
    private static function assertArrayPropertySame($expectedValue, mixed $array, string $path): void
    {
        self::assertIsArray(
            $array,
            'Array expected, got ' . gettype($array) . '.',
        );
        $segments = $path === '' ? [] : explode('.', $path);

        $value = $array;
        foreach ($segments as $i => $segment) {
            // Current node must be an array before accessing the key
            self::assertIsArray(
                $value,
                sprintf(
                    'Path "%s": expected array, got %s.',
                    implode('.', array_slice($segments, 0, $i)),
                    gettype($value),
                ),
            );

            // Key must exist
            self::assertArrayHasKey(
                $segment,
                $value,
                sprintf(
                    'Key "%s" not found at path "%s".',
                    $segment,
                    implode('.', array_slice($segments, 0, $i)),
                ),
            );

            /** @var mixed $value */
            $value = $value[$segment];
        }

        // Final value must be scalar and equal to expected
        self::assertIsScalar($value, sprintf('Path "%s": expected scalar value, got %s.', $path, gettype($value)));
        self::assertSame($expectedValue, $value);
    }
}
