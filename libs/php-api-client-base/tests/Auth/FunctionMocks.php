<?php

declare(strict_types=1);

namespace Keboola\ApiClientBase\Tests\Auth;

/**
 * Static control surface for the namespaced built-in shadows declared in
 * tests/functionOverrides.php. Tests enable it to script file_get_contents() /
 * is_readable() results and to capture usleep() delays without actually sleeping.
 */
final class FunctionMocks
{
    public static bool $enabled = false;
    public static bool $isReadable = true;

    /** @var list<string|false> */
    private static array $readResults = [];
    private static int $readIndex = 0;

    /** @var list<int> */
    private static array $sleeps = [];

    /**
     * @param list<string|false> $readResults Returned by file_get_contents() in order; past the
     *     end it yields false.
     */
    public static function enable(array $readResults, bool $isReadable = true): void
    {
        self::$enabled = true;
        self::$isReadable = $isReadable;
        self::$readResults = array_values($readResults);
        self::$readIndex = 0;
        self::$sleeps = [];
    }

    public static function reset(): void
    {
        self::$enabled = false;
        self::$isReadable = true;
        self::$readResults = [];
        self::$readIndex = 0;
        self::$sleeps = [];
    }

    public static function nextReadResult(): string|false
    {
        $result = self::$readResults[self::$readIndex] ?? false;
        self::$readIndex++;

        return $result;
    }

    public static function recordSleep(int $microseconds): void
    {
        self::$sleeps[] = $microseconds;
    }

    /**
     * @return list<int>
     */
    public static function recordedSleeps(): array
    {
        return self::$sleeps;
    }

    public static function readCount(): int
    {
        return self::$readIndex;
    }
}
