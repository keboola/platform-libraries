<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Workspace\Configuration;

use Keboola\StagingProvider\Exception\StagingProviderException;
use Keboola\StagingProvider\Staging\StagingType;
use Keboola\StorageApi\WorkspaceLoginType;

readonly class WorkspaceBackendConfig
{
    public function __construct(
        private StagingType $stagingType,
        private ?string $size,
        private ?bool $useReadonlyRole,
        private NetworkPolicy $networkPolicy,
        private ?WorkspaceLoginType $loginType,
    ) {
    }

    public function getStagingType(): StagingType
    {
        return $this->stagingType;
    }

    public function getStorageApiWorkspaceType(): string
    {
        return match ($this->stagingType) {
            StagingType::WorkspaceBigquery => 'bigquery',
            StagingType::WorkspaceSnowflake => 'snowflake',

            // @phpstan-ignore-next-line phpdoc is not reliable, stagingType can be any string
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

    public function getLoginType(): ?WorkspaceLoginType
    {
        return $this->loginType;
    }
}
