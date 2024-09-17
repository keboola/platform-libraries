<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Provider;

use Keboola\InputMapping\Staging\ProviderInterface;
use Keboola\StagingProvider\Exception\StagingProviderException;

abstract class AbstractWorkspaceProvider implements ProviderInterface
{
    abstract protected function getWorkspace(): StorageApiWorkspace;

    public function getWorkspaceId(): string
    {
        return $this->getWorkspace()->id;
    }

    public function getCredentials(): array
    {
        return $this->getWorkspace()->credentials;
    }

    public function getBackendSize(): ?string
    {
        return $this->getWorkspace()->backendSize;
    }

    public function getPath(): string
    {
        throw new StagingProviderException('Workspace staging provider does not support path.');
    }
}
