<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Provider;

use Keboola\InputMapping\Staging\ProviderInterface;
use Keboola\StagingProvider\Exception\StagingProviderException;

readonly class LocalStagingProvider implements ProviderInterface
{
    public function __construct(private string $path)
    {
    }

    public function getWorkspaceId(): string
    {
        throw new StagingProviderException('Local staging provider does not support workspace ID.');
    }

    public function getCredentials(): array
    {
        throw new StagingProviderException('Local staging provider does not support workspace credentials.');
    }

    public function getPath(): string
    {
        return $this->path;
    }

    public function cleanup(): void
    {
        // do nothing
    }
}
