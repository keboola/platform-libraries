<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Staging;

use Keboola\InputMapping\Staging\AbstractStagingDefinition;
use Keboola\InputMapping\Staging\StagingInterface;
use Keboola\OutputMapping\Writer\File\StrategyInterface as FileStrategyInterface;
use Keboola\OutputMapping\Writer\Table\StrategyInterface as TableStrategyInterface;

/**
 * @extends AbstractStagingDefinition<TableStrategyInterface, FileStrategyInterface>
 */
class OutputMappingStagingDefinition extends AbstractStagingDefinition
{
    /**
     * @param class-string<FileStrategyInterface> $fileStagingClass
     * @param class-string<TableStrategyInterface> $tableStagingClass
     */
    public function __construct(
        string $name,
        string $fileStagingClass,
        string $tableStagingClass,
        ?StagingInterface $fileDataProvider = null,
        ?StagingInterface $fileMetadataProvider = null,
        ?StagingInterface $tableDataProvider = null,
        ?StagingInterface $tableMetadataProvider = null,
    ) {
        parent::__construct(
            $name,
            $fileStagingClass,
            $tableStagingClass,
            $fileDataProvider,
            $fileMetadataProvider,
            $tableDataProvider,
            $tableMetadataProvider,
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
