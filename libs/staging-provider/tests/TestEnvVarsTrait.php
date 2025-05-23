<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Tests;

use RuntimeException;

trait TestEnvVarsTrait
{
    abstract public static function assertNotEmpty($actual, string $message = ''): void;

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
        $value = (string) getenv($name);
        if ($value === '') {
            throw new RuntimeException(sprintf('Environment variable "%s" is not set.', $name));
        }

        return $value;
    }

    /**
     * @param non-empty-string $name
     * @param string|null $value
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
