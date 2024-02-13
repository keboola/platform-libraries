<?php

declare(strict_types=1);

namespace Keboola\SandboxesServiceApiClient\Sandboxes\Model;

class CreateSandboxPayload
{
    public function __construct(
        public readonly string $componentId,
        public readonly string $configurationId,
        public readonly string $configurationVersion,
        public readonly string $type,
    ) {
    }
}
