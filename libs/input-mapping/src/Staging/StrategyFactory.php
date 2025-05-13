<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Staging;

use Keboola\InputMapping\File\Strategy\Local as FileLocal;
use Keboola\InputMapping\File\StrategyInterface as FileStrategyInterface;
use Keboola\InputMapping\State\InputFileStateList;
use Keboola\InputMapping\State\InputTableStateList;
use Keboola\InputMapping\Table\Strategy\ABS as TableABS;
use Keboola\InputMapping\Table\Strategy\BigQuery as TableBigQuery;
use Keboola\InputMapping\Table\Strategy\Local as TableLocal;
use Keboola\InputMapping\Table\Strategy\S3 as TableS3;
use Keboola\InputMapping\Table\Strategy\Snowflake as TableSnowflake;
use Keboola\InputMapping\Table\StrategyInterface as TableStrategyInterface;
use Keboola\StagingProvider\Mapping\AbstractStrategyMap;
use Keboola\StagingProvider\Mapping\StagingDefinition;
use Keboola\StagingProvider\Staging\StagingProvider;
use Keboola\StagingProvider\Staging\StagingType;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\LoggerInterface;

/**
 * @extends AbstractStrategyMap<FileStrategyInterface, TableStrategyInterface>
 */
class StrategyFactory extends AbstractStrategyMap
{
    public function __construct(
        private readonly StagingProvider $stagingProvider,
        private readonly ClientWrapper $clientWrapper,
        private readonly LoggerInterface $logger,
        private readonly string $format,
    ) {
    }

    protected function provideStagingDefinitions(): iterable
    {
        yield new StagingDefinition(
            type: StagingType::Local,
            fileStrategyClass: FileLocal::class,
            tableStrategyClass: TableLocal::class,
        );

        yield new StagingDefinition(
            type: StagingType::S3,
            fileStrategyClass: FileLocal::class,
            tableStrategyClass: TableS3::class,
        );

        yield new StagingDefinition(
            type: StagingType::Abs,
            fileStrategyClass: FileLocal::class,
            tableStrategyClass: TableABS::class,
        );

        yield new StagingDefinition(
            type: StagingType::WorkspaceSnowflake,
            fileStrategyClass: FileLocal::class,
            tableStrategyClass: TableSnowflake::class,
        );

        yield new StagingDefinition(
            type: StagingType::WorkspaceBigquery,
            fileStrategyClass: FileLocal::class,
            tableStrategyClass: TableBigQuery::class,
        );
    }

    public function getFileInputStrategy(
        InputFileStateList $fileStateList,
    ): FileStrategyInterface {
        $stagingType = $this->stagingProvider->getStagingType();
        $strategyConfiguration = $this->getStagingDefinition($stagingType);
        $this->logger->info(sprintf('Using "%s" file input staging.', $strategyConfiguration->type->value));
        $className = $strategyConfiguration->fileStrategyClass;

        return new $className(
            $this->clientWrapper,
            $this->logger,
            $this->stagingProvider->getFileDataStaging(),
            $this->stagingProvider->getFileMetadataStaging(),
            $fileStateList,
            $this->format,
        );
    }

    public function getTableInputStrategy(
        string $destination,
        InputTableStateList $tablesState,
    ): TableStrategyInterface {
        $stagingType = $this->stagingProvider->getStagingType();
        $strategyConfiguration = $this->getStagingDefinition($stagingType);
        $this->logger->info(sprintf('Using "%s" table input staging.', $strategyConfiguration->type->value));
        $className = $strategyConfiguration->tableStrategyClass;

        return new $className(
            $this->clientWrapper,
            $this->logger,
            $this->stagingProvider->getTableDataStaging(),
            $this->stagingProvider->getTableMetadataStaging(),
            $tablesState,
            $destination,
            $this->format,
        );
    }
}
