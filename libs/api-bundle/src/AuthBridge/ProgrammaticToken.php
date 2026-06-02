<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\AuthBridge;

/**
 * Detects Connection programmatic bearer tokens by their well-known prefixes:
 * - kbc_at_*  access tokens (programmatic login sessions)
 * - kbc_pat_* personal access tokens
 */
final class ProgrammaticToken
{
    /** @var list<non-empty-string> */
    public const PREFIXES = ['kbc_at_', 'kbc_pat_'];

    public static function matches(string $token): bool
    {
        foreach (self::PREFIXES as $prefix) {
            if (str_starts_with($token, $prefix)) {
                return true;
            }
        }

        return false;
    }
}
