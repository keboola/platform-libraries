<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Staging;

use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Writer\File\Strategy\Local as FileLocal;
use Keboola\OutputMapping\Writer\File\StrategyInterface as FileStrategyInterface;
use Keboola\OutputMapping\Writer\Table\Strategy\LocalTableStrategy;
use Keboola\OutputMapping\Writer\Table\Strategy\SqlWorkspaceTableStrategy;
use Keboola\OutputMapping\Writer\Table\StrategyInterface as TableStrategyInterface;
use Keboola\StagingProvider\Staging\File\FileFormat;
use Keboola\StagingProvider\Staging\StagingClass;
use Keboola\StagingProvider\Staging\StagingProvider;
use Keboola\StagingProvider\Staging\StagingType;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\LoggerInterface;

class StrategyFactory
{
    public function __construct(
        private readonly StagingProvider $stagingProvider,
        private readonly ClientWrapper $clientWrapper,
        private readonly LoggerInterface $logger,
        private readonly FileFormat $format,
    ) {
        $stagingType = $this->stagingProvider->getStagingType();
        if ($stagingType->getStagingClass() === StagingClass::File &&
            $stagingType !== StagingType::Local
        ) {
            throw new InvalidOutputException(sprintf(
                'Staging type "%s" is not supported for file output.',
                $stagingType->value,
            ));
        }
    }

    public function getFileOutputStrategy(): FileStrategyInterface
    {
        $stagingType = $this->stagingProvider->getStagingType();
        $this->logger->info(sprintf('Using "%s" file output staging.', $stagingType->value));

        return new ($this->resolveFileStrategyClass($stagingType))(
            $this->clientWrapper,
            $this->logger,
            $this->stagingProvider->getFileDataStaging(),
            $this->stagingProvider->getFileMetadataStaging(),
            $this->format,
        );
    }

    public function getTableOutputStrategy(
        bool $isFailedJob = false,
    ): TableStrategyInterface {
        $stagingType = $this->stagingProvider->getStagingType();
        $this->logger->info(sprintf('Using "%s" table output staging.', $stagingType->value));

        return new ($this->resolveTableStrategyClass($stagingType))(
            $this->clientWrapper,
            $this->logger,
            $this->stagingProvider->getTableDataStaging(),
            $this->stagingProvider->getTableMetadataStaging(),
            $this->format,
            $isFailedJob,
        );
    }

    /**
     * @return class-string<FileStrategyInterface>
     */
    private function resolveFileStrategyClass(StagingType $stagingType): string
    {
        return match ($stagingType) {
            StagingType::Local,
            StagingType::WorkspaceSnowflake,
            StagingType::WorkspaceBigquery => FileLocal::class,

            default => throw new InvalidOutputException(sprintf(
                'File output mapping is not supported for "%s" staging.',
                $stagingType->value,
            )),
        };
    }

    /**
     * @return class-string<TableStrategyInterface>
     */
    public function resolveTableStrategyClass(StagingType $stagingType): string
    {
        return match ($stagingType) {
            StagingType::Local => LocalTableStrategy::class,

            StagingType::WorkspaceSnowflake,
            StagingType::WorkspaceBigquery => SqlWorkspaceTableStrategy::class,

            default => throw new InvalidOutputException(sprintf(
                'Table output mapping is not supported for "%s" staging.',
                $stagingType->value,
            )),
        };
    }
}
