<?php

declare(strict_types=1);

namespace Keboola\QueryApi\Tests;

trait TestEnvVarsTrait
{
    /**
     * @phpstan-assert non-empty-string $actual
     */
    abstract public static function assertNotEmpty(mixed $actual, string $message = ''): void;

    /**
     * @return non-empty-string|null
     */
    protected static function getOptionalEnv(string $name): ?string
    {
        $value = (string) getenv($name);
        return $value !== '' ? $value : null;
    }

    /**
     * @return non-empty-string
     */
    protected static function getRequiredEnv(string $name): string
    {
        $value = getenv($name);
        self::assertNotEmpty($value, sprintf('Environment variable "%s" is not set.', $name));
        /** @var non-empty-string $value */
        return $value;
    }

    /**
     * @param non-empty-string $name
     */
    protected static function overrideEnv(string $name, ?string $value): void
    {
        if ($value === null) {
            unset($_ENV[$name]);
            putenv($name);
        } else {
            $_ENV[$name] = $value;
            putenv(sprintf('%s=%s', $name, $value));
        }
    }
}
