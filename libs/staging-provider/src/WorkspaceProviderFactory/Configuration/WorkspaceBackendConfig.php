<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\WorkspaceProviderFactory\Configuration;

class WorkspaceBackendConfig
{
    public function __construct(private readonly ?string $type)
    {
    }

    public function getType(): ?string
    {
        return $this->type;
    }
}
