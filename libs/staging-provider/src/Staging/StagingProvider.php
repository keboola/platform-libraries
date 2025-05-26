<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Staging;

use InvalidArgumentException;
use Keboola\StagingProvider\Staging\File\LocalStaging;
use Keboola\StagingProvider\Staging\Workspace\WorkspaceStaging;

class StagingProvider
{
    private readonly StagingType $stagingType;
    private readonly LocalStaging $localStaging;
    private readonly LocalStaging|WorkspaceStaging $tableDataStaging;

    public function __construct(
        StagingType $stagingType,
        string $localStagingPath,
        ?string $stagingWorkspaceId,
    ) {
        $isWorkspaceStaging = $stagingType->getStagingClass() === StagingClass::Workspace;
        $hasWorkspaceId = $stagingWorkspaceId !== null;
        if ($isWorkspaceStaging !== $hasWorkspaceId) {
            throw new InvalidArgumentException(
                'Staging workspace ID must be configured (only) with workspace staging.',
            );
        }

        $this->stagingType = $stagingType;
        $this->localStaging = new LocalStaging($localStagingPath);
        $this->tableDataStaging = match ($stagingType->getStagingClass()) {
            StagingClass::Disk => $this->localStaging,
            StagingClass::Workspace => new WorkspaceStaging($stagingWorkspaceId),
        };
    }

    public function getStagingType(): StagingType
    {
        return $this->stagingType;
    }

    public function getFileDataStaging(): StagingInterface
    {
        return $this->localStaging;
    }

    public function getFileMetadataStaging(): StagingInterface
    {
        return $this->localStaging;
    }

    public function getTableDataStaging(): StagingInterface
    {
        return $this->tableDataStaging;
    }

    public function getTableMetadataStaging(): StagingInterface
    {
        return $this->localStaging;
    }
}
