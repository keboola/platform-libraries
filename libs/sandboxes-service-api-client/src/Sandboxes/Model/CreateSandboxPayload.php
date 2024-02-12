<?php

declare(strict_types=1);

namespace Keboola\SandboxesServiceApiClient\Sandboxes\Model;

class CreateSandboxPayload
{
    public function __construct(
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
}
