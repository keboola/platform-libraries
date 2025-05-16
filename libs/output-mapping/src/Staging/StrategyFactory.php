<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Staging;

use InvalidArgumentException;
use Keboola\OutputMapping\Writer\File\Strategy\Local;
use Keboola\OutputMapping\Writer\File\StrategyInterface as FileStrategyInterface;
use Keboola\OutputMapping\Writer\Table\Strategy\LocalTableStrategy;
use Keboola\OutputMapping\Writer\Table\Strategy\SqlWorkspaceTableStrategy;
use Keboola\OutputMapping\Writer\Table\StrategyInterface as TableStrategyInterface;
use Keboola\StagingProvider\Mapping\AbstractStrategyMap;
use Keboola\StagingProvider\Mapping\StagingDefinition;
use Keboola\StagingProvider\Staging\StagingClass;
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
            fileStrategyClass: Local::class,
            tableStrategyClass: LocalTableStrategy::class,
        );

        yield new StagingDefinition(
            type: StagingType::WorkspaceSnowflake,
            fileStrategyClass: Local::class,
            tableStrategyClass: SqlWorkspaceTableStrategy::class,
        );

        yield new StagingDefinition(
            type: StagingType::WorkspaceBigquery,
            fileStrategyClass: Local::class,
            tableStrategyClass: SqlWorkspaceTableStrategy::class,
        );
    }

    public function getFileOutputStrategy(): FileStrategyInterface
    {
        $stagingType = $this->stagingProvider->getStagingType();
        $stagingDefinition = $this->getStagingDefinition($stagingType);
        $this->logger->info(sprintf('Using "%s" file output staging.', $stagingDefinition->type->value));

        return new ($stagingDefinition->fileStrategyClass)(
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
        if ($stagingType->getStagingClass() === StagingClass::File && $stagingType !== StagingType::Local) {
            throw new InvalidArgumentException(sprintf(
                'Staging type "%s" is not supported for table output.',
                $stagingType->value,
            ));
        }

        $stagingDefinition = $this->getStagingDefinition($stagingType);
        $this->logger->info(sprintf('Using "%s" table output staging.', $stagingDefinition->type->value));

        return new ($stagingDefinition->tableStrategyClass)(
            $this->clientWrapper,
            $this->logger,
            $this->stagingProvider->getTableDataStaging(),
            $this->stagingProvider->getTableMetadataStaging(),
            $this->format,
            $isFailedJob,
        );
    }
}
