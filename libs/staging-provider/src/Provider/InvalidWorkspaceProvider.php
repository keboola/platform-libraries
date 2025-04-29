<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Provider;

use Keboola\StagingProvider\Exception\StagingProviderException;

class InvalidWorkspaceProvider implements WorkspaceProviderInterface
{
    public function __construct(
        private readonly string $stagingType,
    ) {
    }

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

    public function getPath(): never
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

    private function throwError(): never
    {
        throw new StagingProviderException(sprintf(
            'No workspace provider is available for staging type "%s"',
            $this->stagingType,
        ));
    }
}
