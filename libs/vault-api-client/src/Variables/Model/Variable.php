<?php

declare(strict_types=1);

namespace Keboola\VaultApiClient\Variables\Model;

use Keboola\VaultApiClient\ResponseModelInterface;

final readonly class Variable implements ResponseModelInterface
{
    public function __construct(
        public string $hash,
        public string $key,
        public string $value,
        public bool $isEncrypted,
        public array $attributes,
    ) {
    }

    public static function fromResponseData(array $data): static
    {
        return new self(
            $data['hash'],
            $data['key'],
            $data['value'],
            $data['isEncrypted'] ?? false,
            $data['attributes'] ?? [],
        );
    }
}
