<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Staging;

use Keboola\InputMapping\File\StrategyInterface as FileStrategyInterface;
use Keboola\InputMapping\Table\StrategyInterface as TableStrategyInterface;

class InputMappingStagingDefinition extends AbstractStagingDefinition
{
    /** @var class-string<FileStrategyInterface> */
    protected string $fileStagingClass;
    /** @var class-string<TableStrategyInterface> */
    protected string $tableStagingClass;

    /**
     * @param class-string<FileStrategyInterface> $fileStagingClass
     * @param class-string<TableStrategyInterface> $tableStagingClass
     */
    public function __construct(
        string $name,
        string $fileStagingClass,
        string $tableStagingClass,
        ?ProviderInterface $fileDataProvider = null,
        ?ProviderInterface $fileMetadataProvider = null,
        ?ProviderInterface $tableDataProvider = null,
        ?ProviderInterface $tableMetadataProvider = null,
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
        $this->fileStagingClass = $fileStagingClass;
        $this->tableStagingClass = $tableStagingClass;
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
