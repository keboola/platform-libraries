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
use Keboola\StagingProvider\Staging\File\FileFormat;
use Keboola\StagingProvider\Staging\StagingProvider;
use Keboola\StagingProvider\Staging\StagingType;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\LoggerInterface;

class StrategyFactory
{
    /** @var class-string<FileStrategyInterface> */
    private readonly string $fileStrategyClass;

    /** @var class-string<TableStrategyInterface>  */
    private readonly string $tableStrategyClass;

    public function __construct(
        private readonly StagingProvider $stagingProvider,
        private readonly ClientWrapper $clientWrapper,
        private readonly LoggerInterface $logger,
        private readonly FileFormat $format,
    ) {
        $stagingType = $this->stagingProvider->getStagingType();

        $this->fileStrategyClass = match ($stagingType) {
            StagingType::Local,
            StagingType::S3,
            StagingType::Abs,
            StagingType::WorkspaceSnowflake,
            StagingType::WorkspaceBigquery => FileLocal::class,
        };

        $this->tableStrategyClass = match ($stagingType) {
            StagingType::Local => TableLocal::class,
            StagingType::S3 => TableS3::class,
            StagingType::Abs => TableABS::class,
            StagingType::WorkspaceSnowflake => TableSnowflake::class,
            StagingType::WorkspaceBigquery => TableBigQuery::class,
        };
    }

    public function getFileInputStrategy(
        InputFileStateList $fileStateList,
    ): FileStrategyInterface {
        $stagingType = $this->stagingProvider->getStagingType();
        $this->logger->info(sprintf('Using "%s" file input staging.', $stagingType->value));

        return new ($this->fileStrategyClass)(
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
        $this->logger->info(sprintf('Using "%s" table input staging.', $stagingType->value));

        return new ($this->tableStrategyClass)(
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
