<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Staging;

use Keboola\StagingProvider\Staging\File\FileStagingInterface;
use Keboola\StagingProvider\Staging\Workspace\WorkspaceStagingInterface;

class StagingProvider
{
    private StagingInterface $tableDataStaging;

    public function __construct(
        private readonly StagingType $stagingType,
        WorkspaceStagingInterface $stagingWorkspace,
        private readonly FileStagingInterface $localStaging,
    ) {
        $this->tableDataStaging = match ($this->stagingType->getStagingClass()) {
            // TABLE_DATA for ABS and S3 is bound to LocalProvider because it requires no provider at all
            StagingClass::File => $this->localStaging,
            StagingClass::Workspace => $stagingWorkspace,
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

    public function getWorkspaceStaging(): ?WorkspaceStagingInterface
    {
        return $this->tableDataStaging instanceof WorkspaceStagingInterface ? $this->tableDataStaging : null;
    }
}
