<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Staging;

use Keboola\StagingProvider\Exception\NoStagingAvailableException;
use Keboola\StagingProvider\Staging\File\FileStagingInterface;
use Keboola\StagingProvider\Staging\Workspace\WorkspaceStagingInterface;

class StagingProvider
{
    private readonly ?StagingInterface $tableDataStaging;

    public function __construct(
        private readonly StagingType $stagingType,
        ?WorkspaceStagingInterface $workspaceStaging,
        private readonly ?FileStagingInterface $localStaging,
    ) {
        $this->tableDataStaging = match ($stagingType->getStagingClass()) {
            // TABLE_DATA for ABS and S3 is bound to LocalProvider because it requires no provider at all
            StagingClass::Disk => $localStaging,
            StagingClass::Workspace => $workspaceStaging,
        };
    }

    public function getStagingType(): StagingType
    {
        return $this->stagingType;
    }

    public function getFileDataStaging(): StagingInterface
    {
        if ($this->localStaging === null) {
            throw new NoStagingAvailableException(sprintf(
                'Undefined file data provider in "%s" staging.',
                $this->stagingType->name,
            ));
        }

        return $this->localStaging;
    }

    public function getFileMetadataStaging(): StagingInterface
    {
        if ($this->localStaging === null) {
            throw new NoStagingAvailableException(sprintf(
                'Undefined file metadata provider in "%s" staging.',
                $this->stagingType->name,
            ));
        }

        return $this->localStaging;
    }

    public function getTableDataStaging(): StagingInterface
    {
        if ($this->tableDataStaging === null) {
            throw new NoStagingAvailableException(sprintf(
                'Undefined table data provider in "%s" staging.',
                $this->stagingType->name,
            ));
        }

        return $this->tableDataStaging;
    }

    public function getTableMetadataStaging(): StagingInterface
    {
        if ($this->localStaging === null) {
            throw new NoStagingAvailableException(sprintf(
                'Undefined table metadata provider in "%s" staging.',
                $this->stagingType->name,
            ));
        }

        return $this->localStaging;
    }
}
