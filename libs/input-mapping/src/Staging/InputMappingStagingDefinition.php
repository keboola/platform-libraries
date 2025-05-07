<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Staging;

use Keboola\InputMapping\File\StrategyInterface as FileStrategyInterface;
use Keboola\InputMapping\Table\StrategyInterface as TableStrategyInterface;

/**
 * @extends AbstractStagingDefinition<TableStrategyInterface, FileStrategyInterface>
 */
class InputMappingStagingDefinition extends AbstractStagingDefinition
{
    public function __construct(
        string $name,
        string $fileStagingClass,
        string $tableStagingClass,
        ?StagingInterface $fileDataStaging = null,
        ?StagingInterface $fileMetadataStaging = null,
        ?StagingInterface $tableDataStaging = null,
        ?StagingInterface $tableMetadataStaging = null,
    ) {
        parent::__construct(
            $name,
            $fileStagingClass,
            $tableStagingClass,
            $fileDataStaging,
            $fileMetadataStaging,
            $tableDataStaging,
            $tableMetadataStaging,
        );
    }

    /**
     * @return class-string<FileStrategyInterface>
     */
    public function getFileStagingClass(): string
    {
        return $this->fileStagingClass;
    }

    /**
     * @return class-string<TableStrategyInterface>
     */
    public function getTableStagingClass(): string
    {
        return $this->tableStagingClass;
    }
}
