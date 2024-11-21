<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Provider\Configuration;

use Keboola\InputMapping\Staging\AbstractStrategyFactory;
use Keboola\StagingProvider\Exception\StagingProviderException;

readonly class WorkspaceBackendConfig
{
    public function __construct(
        private string $stagingType,
        private ?string $size,
        private ?bool $useReadonlyRole,
        private NetworkPolicy $networkPolicy,
    ) {
    }

    public function getStagingType(): string
    {
        return $this->stagingType;
    }

    public function getStorageApiWorkspaceType(): string
    {
        return match ($this->stagingType) {
            AbstractStrategyFactory::WORKSPACE_ABS => 'abs',
            AbstractStrategyFactory::WORKSPACE_BIGQUERY => 'bigquery',
            AbstractStrategyFactory::WORKSPACE_EXASOL => 'exasol',
            AbstractStrategyFactory::WORKSPACE_REDSHIFT => 'redshift',
            AbstractStrategyFactory::WORKSPACE_SNOWFLAKE => 'snowflake',
            AbstractStrategyFactory::WORKSPACE_SYNAPSE => 'synapse',
            AbstractStrategyFactory::WORKSPACE_TERADATA => 'teradata',
            default => throw new StagingProviderException(sprintf('Unknown staging type "%s"', $this->stagingType)),
        };
    }

    public function getStorageApiWorkspaceSize(): ?string
    {
        return $this->size;
    }

    public function getUseReadonlyRole(): ?bool
    {
        return $this->useReadonlyRole;
    }

    public function getNetworkPolicy(): string
    {
        return $this->networkPolicy->value;
    }
}
