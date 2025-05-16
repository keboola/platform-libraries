<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Staging;

use Keboola\InputMapping\Exception\InvalidInputException;
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
use Keboola\StagingProvider\Staging\File\FileFormat;
use Keboola\StagingProvider\Staging\File\FileStagingInterface;
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
    }

    public function getFileInputStrategy(
        InputFileStateList $fileStateList,
    ): FileStrategyInterface {
        $this->logger->info(sprintf('Using "%s" file input staging.', $this->stagingType->value));

        return new ($this->resolveFileStrategyClass())(
            $this->clientWrapper,
            $this->logger,
            $this->getFileDataStaging(),
            $this->getFileMetadataStaging(),
            $fileStateList,
            $this->format,
        );
    }

    public function getTableInputStrategy(
        string $destination,
        InputTableStateList $tablesState,
    ): TableStrategyInterface {
        $this->logger->info(sprintf('Using "%s" table input staging.', $this->stagingType->value));

        return new ($this->resolveTableStrategyClass())(
            $this->clientWrapper,
            $this->logger,
            $this->getTableDataStaging(),
            $this->getTableMetadataStaging(),
            $tablesState,
            $destination,
            $this->format,
        );
    }

    /**
     * @return class-string<FileStrategyInterface>
     */
    private function resolveFileStrategyClass(): string
    {
        return match ($this->stagingType) {
            StagingType::Local,
            StagingType::S3,
            StagingType::Abs,
            StagingType::WorkspaceSnowflake,
            StagingType::WorkspaceBigquery => FileLocal::class,

            // @phpstan-ignore-next-line - keep the "default" eve though all staging types are covered
            default => throw new InvalidInputException(sprintf(
                'Input mapping on type "%s" is not supported.',
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
            StagingType::Local => TableLocal::class,
            StagingType::S3 => TableS3::class,
            StagingType::Abs => TableABS::class,
            StagingType::WorkspaceSnowflake => TableSnowflake::class,
            StagingType::WorkspaceBigquery => TableBigQuery::class,

            // @phpstan-ignore-next-line - keep the "default" eve though all staging types are covered
            default => throw new InvalidInputException(sprintf(
                'Input mapping on type "%s" is not supported.',
                $this->stagingType->value,
            )),
        };
    }
}
