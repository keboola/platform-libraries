<?php

declare(strict_types=1);

namespace Keboola\VaultApiClient\Variables\Model;

use Keboola\ApiClientBase\ResponseModelInterface;
use Webmozart\Assert\Assert;

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

    /**
     * @param array<string, mixed> $data
     */
    public static function fromResponseData(array $data): static
    {
        Assert::stringNotEmpty($data['hash']);
        Assert::stringNotEmpty($data['key']);
        Assert::string($data['value']);

        $flags = (array) ($data['flags'] ?? []);
        Assert::allString($flags);

        $attributes = (array) ($data['attributes'] ?? []);
        Assert::isMap($attributes);
        Assert::allString($attributes);

        /** @var array<self::FLAG_*> $flags */
        /** @var array<non-empty-string, string> $attributes */
        return new self(
            $data['hash'],
            $data['key'],
            $data['value'],
            $flags,
            $attributes,
        );
    }
}
