<?php

declare(strict_types=1);

namespace Keboola\SandboxesServiceApiClient\Sandboxes\Model;

use Keboola\AzureApiClient\Marketplace\Model\Subscription;
use Keboola\SandboxesServiceApiClient\ResponseModelInterface;

class CreateSandboxResult implements ResponseModelInterface
{
    public function __construct(
        public readonly string $id,
        public readonly string $projectId,
        public readonly string $tokenId,
        public readonly string $configurationId,
        public readonly string $type,
        public readonly string $size,
        public readonly string $sizeParameters,
        public readonly string $user,
        public readonly string $password,
        public readonly string $host,
        public readonly string $url,
        public readonly string $imageVersion,
    ) {
    }

    public static function fromResponseData(array $data): static
    {
        return new self(
            $data['id'],
            $data['projectId'],
            $data['tokenId'],
            $data['configurationId'],
            $data['type'],
            $data['size'],
            $data['sizeParameters'],
            $data['user'],
            $data['password'],
            $data['host'],
            $data['url'],
            $data['imageVersion'],
        );
    }
}
