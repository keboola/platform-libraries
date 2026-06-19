<?php

declare(strict_types=1);

namespace Keboola\ConfigurationVariablesResolver;

/**
 * Configuration for the vault VariablesApiClient.
 *
 * Re-introduced locally after vault-api-client dropped its own ApiClientConfiguration
 * (flattened into individual constructor arguments). Keeps this lib's previous default
 * of 10 retries and preserves the factory's config-object API for its consumers.
 */
class VaultVariablesApiClientConfiguration
{
    private const DEFAULT_BACKOFF_RETRIES = 10;

    /**
     * @param int<0, max> $backoffMaxTries
     */
    public function __construct(
        public readonly ?string $userAgent = null,
        public readonly int $backoffMaxTries = self::DEFAULT_BACKOFF_RETRIES,
    ) {
    }
}
