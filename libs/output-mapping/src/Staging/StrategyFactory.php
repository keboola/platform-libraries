<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Staging;

use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Writer\File\Strategy\Local as FileLocal;
use Keboola\OutputMapping\Writer\File\StrategyInterface as FileStrategyInterface;
use Keboola\OutputMapping\Writer\Table\Strategy\LocalTableStrategy;
use Keboola\OutputMapping\Writer\Table\Strategy\SqlWorkspaceTableStrategy;
use Keboola\OutputMapping\Writer\Table\StrategyInterface as TableStrategyInterface;
use Keboola\StagingProvider\Mapping\AbstractStrategyMap;
use Keboola\StagingProvider\Staging\File\FileFormat;
use Keboola\StagingProvider\Staging\File\FileStagingInterface;
use Keboola\StagingProvider\Staging\StagingClass;
use Keboola\StagingProvider\Staging\StagingType;
use Keboola\StagingProvider\Staging\Workspace\WorkspaceStagingInterface;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\LoggerInterface;

class StrategyFactory extends AbstractStrategyMap
{
    public function __construct(
        StagingType $stagingType,
        ?WorkspaceStagingInterface $stagingWorkspace,
        ?FileStagingInterface $localStaging,
        private readonly ClientWrapper $clientWrapper,
        private readonly LoggerInterface $logger,
        private readonly FileFormat $format,
    ) {
        parent::__construct(
            $stagingType,
            $stagingWorkspace,
            $localStaging,
        );

        if ($stagingType->getStagingClass() === StagingClass::File &&
            $stagingType !== StagingType::Local
        ) {
            throw new InvalidOutputException(sprintf(
                'Staging type "%s" is not supported for table output.',
                $stagingType->value,
            ));
        }
    }

    public function getFileOutputStrategy(): FileStrategyInterface
    {
        $this->logger->info(sprintf('Using "%s" file output staging.', $this->stagingType->value));

        return new ($this->resolveFileStrategyClass())(
            $this->clientWrapper,
            $this->logger,
            $this->getFileDataStaging(),
            $this->getFileMetadataStaging(),
            $this->format,
        );
    }

    public function getTableOutputStrategy(
        bool $isFailedJob = false,
    ): TableStrategyInterface {
        $this->logger->info(sprintf('Using "%s" table output staging.', $this->stagingType->value));

        return new ($this->resolveTableStrategyClass())(
            $this->clientWrapper,
            $this->logger,
            $this->getTableDataStaging(),
            $this->getTableMetadataStaging(),
            $this->format,
            $isFailedJob,
        );
    }

    /**
     * @return class-string<FileStrategyInterface>
     */
    private function resolveFileStrategyClass(): string
    {
        return match ($this->stagingType) {
            StagingType::Local,
            StagingType::WorkspaceSnowflake,
            StagingType::WorkspaceBigquery => FileLocal::class,

            default => throw new InvalidOutputException(sprintf(
                'Output mapping on type "%s" is not supported.',
                $this->stagingType->value,
            )),
        };
    }

    /**
     * @return class-string<TableStrategyInterface>
     */
    public function resolveTableStrategyClass(): string
    {
        return match ($this->stagingType) {
            StagingType::Local => LocalTableStrategy::class,
            StagingType::WorkspaceSnowflake => SqlWorkspaceTableStrategy::class,
            StagingType::WorkspaceBigquery => SqlWorkspaceTableStrategy::class,

            default => throw new InvalidOutputException(sprintf(
                'Output mapping on type "%s" is not supported.',
                $this->stagingType->value,
            )),
        };
    }
}
