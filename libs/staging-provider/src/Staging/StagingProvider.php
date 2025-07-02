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
        $this->stagingType = $stagingType;
        $this->localStaging = new LocalStaging($localStagingPath);

        switch ($stagingType->getStagingClass()) {
            case StagingClass::Workspace:
                if ($stagingWorkspaceId === null) {
                    throw new InvalidArgumentException(
                        'Staging workspace ID must be configured (only) with workspace staging.',
                    );
                }

                $this->tableDataStaging = new WorkspaceStaging($stagingWorkspaceId);
                break;

            case StagingClass::Disk:
                if ($stagingWorkspaceId !== null) {
                    throw new InvalidArgumentException(
                        'Staging workspace ID must be configured (only) with workspace staging.',
                    );
                }

                $this->tableDataStaging = $this->localStaging;
                break;

            default:
                throw new InvalidArgumentException(sprintf(
                    'Stating type "%s" is not supported.',
                    $stagingType->value,
                ));
        }
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
