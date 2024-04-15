<?php

declare(strict_types=1);

namespace Keboola\SandboxesServiceApiClient\Sandboxes\Model;

use Keboola\SandboxesServiceApiClient\ResponseModelInterface;

final readonly class Sandbox implements ResponseModelInterface
{
    public function __construct(
        public string $id,
        public string $projectId,
        public string $tokenId,
        public string $componentId,
        public string $configurationId,
        public string $configurationVersion,
        public string $type,
        public bool $active = false,
    ) {
    }

    public static function fromResponseData(array $data): static
    {
        return new self(
            $data['id'],
            $data['projectId'],
            $data['tokenId'],
            $data['componentId'],
            $data['configurationId'],
            $data['configurationVersion'],
            $data['type'],
            $data['active'] ?? false,
        );
    }
}
