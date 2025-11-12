<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Staging;

use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\OutputMappingSettings;
use Keboola\OutputMapping\Writer\File\Strategy\Local as FileLocal;
use Keboola\OutputMapping\Writer\File\StrategyInterface as FileStrategyInterface;
use Keboola\OutputMapping\Writer\Table\Strategy\LocalTableStrategy;
use Keboola\OutputMapping\Writer\Table\Strategy\SqlWorkspaceTableStrategy;
use Keboola\OutputMapping\Writer\Table\StrategyInterface as TableStrategyInterface;
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
            StagingType::WorkspaceSnowflake,
            StagingType::WorkspaceBigquery => FileLocal::class,

            default => throw new InvalidOutputException(sprintf(
                'File output mapping is not supported for "%s" staging.',
                $stagingType->value,
            )),
        };

        $this->tableStrategyClass = match ($stagingType) {
            StagingType::Local => LocalTableStrategy::class,

            StagingType::WorkspaceSnowflake,
            StagingType::WorkspaceBigquery => SqlWorkspaceTableStrategy::class,

            default => throw new InvalidOutputException(sprintf(
                'Table output mapping is not supported for "%s" staging.',
                $stagingType->value,
            )),
        };
    }

    public function getFileOutputStrategy(): FileStrategyInterface
    {
        $stagingType = $this->stagingProvider->getStagingType();
        $this->logger->info(sprintf('Using "%s" file output staging.', $stagingType->value));

        return new ($this->fileStrategyClass)(
            $this->clientWrapper,
            $this->logger,
            $this->stagingProvider->getFileDataStaging(),
            $this->stagingProvider->getFileMetadataStaging(),
            $this->format,
        );
    }

    public function getTableOutputStrategy(
        OutputMappingSettings $outputMappingSettings,
        bool $isFailedJob = false,
    ): TableStrategyInterface {
        $stagingType = $this->stagingProvider->getStagingType();
        $this->logger->info(sprintf('Using "%s" table output staging.', $stagingType->value));

        return new ($this->tableStrategyClass)(
            $this->clientWrapper,
            $this->logger,
            $this->stagingProvider->getTableDataStaging(),
            $this->stagingProvider->getTableMetadataStaging(),
            $this->format,
            $isFailedJob,
            $outputMappingSettings->getRawConfiguration(),
        );
    }
}
