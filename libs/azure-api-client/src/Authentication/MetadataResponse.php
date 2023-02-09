<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Authentication;

use Keboola\AzureApiClient\ResponseModelInterface;

final class MetadataResponse implements ResponseModelInterface
{
    public function __construct(
        public readonly string $name,
        public readonly string $keyVaultDnsSuffix,
        public readonly string $authenticationLoginEndpoint,
    ) {
    }

    public static function fromResponseData(array $data): static
    {
        return new self(
            (string) $data['name'],
            (string) $data['suffixes']['keyVaultDns'],
            (string) $data['authentication']['loginEndpoint'],
        );
    }
}
