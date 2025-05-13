<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Staging\Workspace;

use Keboola\StagingProvider\Exception\StagingProviderException;

class NullWorkspaceStaging implements WorkspaceStagingInterface
{
    public function getWorkspaceId(): never
    {
        $this->throwError();
    }

    public function cleanup(): never
    {
        $this->throwError();
    }

    public function getCredentials(): never
    {
        $this->throwError();
    }

    public function getBackendSize(): never
    {
        $this->throwError();
    }

    public function getBackendType(): never
    {
        $this->throwError();
    }

    public function resetCredentials(array $params): void
    {
        $this->throwError();
    }

    private function throwError(): never
    {
        throw new StagingProviderException('Workspace staging is not available');
    }
}
