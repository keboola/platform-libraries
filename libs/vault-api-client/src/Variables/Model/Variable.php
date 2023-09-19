<?php

declare(strict_types=1);

namespace Keboola\VaultApiClient\Variables\Model;

use Keboola\VaultApiClient\ResponseModelInterface;

final readonly class Variable implements ResponseModelInterface
{
    public const FLAG_ENCRYPTED = 'encrypted';
    public const FLAG_OAUTH_CREDENTIALS_ID = 'oauthCredentialsId';

    /**
     * @param non-empty-string $hash
     * @param non-empty-string $key
     * @param string $value
     * @param array<self::FLAG_*> $flags
     * @param array<non-empty-string, string> $attributes
     */
    public function __construct(
        public string $hash,
        public string $key,
        public string $value,
        public array $flags,
        public array $attributes,
    ) {
    }

    public static function fromResponseData(array $data): static
    {
        return new self(
            $data['hash'],
            $data['key'],
            $data['value'],
            $data['flags'] ?? [],
            $data['attributes'] ?? [],
        );
    }
}
