<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Workspace\ProviderConfig;

use Keboola\StagingProvider\Exception\StagingProviderException;
use Keboola\StagingProvider\Staging\StagingClass;
use Keboola\StagingProvider\Staging\StagingType;
use Keboola\StagingProvider\Workspace\Configuration\NetworkPolicy;
use Keboola\StorageApi\WorkspaceLoginType;
use Keboola\StorageApiBranch\StorageApiToken;

readonly class NewWorkspaceConfig implements WorkspaceConfigInterface
{
    public function __construct(
        StorageApiToken $storageApiToken,
        public StagingType $stagingType,
        public string $componentId,
        public ?string $configId,
        public ?string $size,
        public ?bool $useReadonlyRole,
        public NetworkPolicy $networkPolicy,
        public ?WorkspaceLoginType $loginType,
    ) {
        if ($this->stagingType->getStagingClass() !== StagingClass::Workspace) {
            throw new StagingProviderException(sprintf(
                'Can\'t create workspace for staging %s',
                $this->stagingType->value,
            ));
        }

        if (!$this->doesTokenHaveStagingConfigured($storageApiToken)) {
            throw new StagingProviderException(sprintf(
                'The project does not support "%s" table backend.',
                $this->stagingType->value,
            ));
        }
    }

    public function getStorageApiWorkspaceType(): string
    {
        return match ($this->stagingType) {
            StagingType::WorkspaceBigquery => 'bigquery',
            StagingType::WorkspaceSnowflake => 'snowflake',

            default => throw new StagingProviderException(sprintf(
                'Unknown staging type "%s"',
                $this->stagingType->value,
            )),
        };
    }

    private function doesTokenHaveStagingConfigured(StorageApiToken $storageApiToken): bool
    {
        $tokenOwnerInfo = $storageApiToken->getTokenInfo()['owner'] ?? [];

        return match ($this->stagingType) {
            StagingType::WorkspaceSnowflake => $tokenOwnerInfo['hasSnowflake'] ?? false,
            StagingType::WorkspaceBigquery => $tokenOwnerInfo['hasBigquery'] ?? false,
            default => false,
        };
    }
}
