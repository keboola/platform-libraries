<?php

declare(strict_types=1);

namespace Keboola\SandboxesServiceApiClient\Sandboxes\Model;

use Keboola\SandboxesServiceApiClient\ResponseModelInterface;

final class CreateSandboxResult implements ResponseModelInterface
{
    public function __construct(
        public readonly string $id,
        public readonly string $projectId,
        public readonly string $tokenId,
        public readonly string $configurationId,
        public readonly string $configurationVersion,
        public readonly string $type,
        public readonly ?string $size = null,
        public readonly ?string $sizeParameters = null,
        public readonly ?string $user = null,
        public readonly ?string $password = null,
        public readonly ?string $host = null,
        public readonly ?string $url = null,
        public readonly ?string $imageVersion = null,
    ) {
    }

    public static function fromResponseData(array $data): static
    {
        return new self(
            $data['id'],
            $data['projectId'],
            $data['tokenId'],
            $data['configurationId'],
            $data['configurationVersion'],
            $data['type'],
            $data['size'] ?? null,
            $data['sizeParameters'] ?? null,
            $data['user'] ?? null,
            $data['password'] ?? null,
            $data['host'] ?? null,
            $data['url'] ?? null,
            $data['imageVersion'] ?? null,
        );
    }
}
