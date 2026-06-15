<?php

declare(strict_types=1);

namespace Keboola\ApiClientBase\Auth;

use Keboola\ApiClientBase\Tests\Auth\FunctionMocks;

// Test-only shadows of the built-in filesystem functions used by
// KeboolaServiceAccountAuthenticator. PHP resolves unqualified calls inside the
// Keboola\ApiClientBase\Auth namespace to these first; they delegate to the real
// built-ins unless FunctionMocks is enabled. Loaded once from tests/bootstrap.php.

/**
 * @param resource|null $context
 * @param int<0, max> $offset
 * @param int<0, max>|null $length
 * @return string|false
 */
function file_get_contents(
    string $filename,
    bool $use_include_path = false,
    $context = null,
    int $offset = 0,
    ?int $length = null,
): string|false {
    if (FunctionMocks::$enabled) {
        return FunctionMocks::nextReadResult();
    }

    if ($length === null) {
        // phpcs:ignore SlevomatCodingStandard.Namespaces.ReferenceUsedNamesOnly.ReferenceViaFullyQualifiedName
        return \file_get_contents($filename, $use_include_path, $context, $offset);
    }

    // phpcs:ignore SlevomatCodingStandard.Namespaces.ReferenceUsedNamesOnly.ReferenceViaFullyQualifiedName
    return \file_get_contents($filename, $use_include_path, $context, $offset, $length);
}

function is_readable(string $filename): bool
{
    if (FunctionMocks::$enabled) {
        return FunctionMocks::$isReadable;
    }

    // phpcs:ignore SlevomatCodingStandard.Namespaces.ReferenceUsedNamesOnly.ReferenceViaFullyQualifiedName
    return \is_readable($filename);
}

function usleep(int $microseconds): void
{
    if (FunctionMocks::$enabled) {
        FunctionMocks::recordSleep($microseconds);

        return;
    }

    // phpcs:ignore SlevomatCodingStandard.Namespaces.ReferenceUsedNamesOnly.ReferenceViaFullyQualifiedName
    \usleep($microseconds);
}
