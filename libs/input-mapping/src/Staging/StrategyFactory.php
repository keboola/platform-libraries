<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Staging;

use Keboola\InputMapping\Exception\InvalidInputException;
use Keboola\InputMapping\Exception\StagingException;
use Keboola\InputMapping\File\Strategy\ABSWorkspace as FileABSWorkspace;
use Keboola\InputMapping\File\Strategy\Local as FileLocal;
use Keboola\InputMapping\File\StrategyInterface as FileStrategyInterface;
use Keboola\InputMapping\State\InputFileStateList;
use Keboola\InputMapping\State\InputTableStateList;
use Keboola\InputMapping\Table\Strategy\ABS as TableABS;
use Keboola\InputMapping\Table\Strategy\ABSWorkspace as TableABSWorkspace;
use Keboola\InputMapping\Table\Strategy\BigQuery as TableBigQuery;
use Keboola\InputMapping\Table\Strategy\Exasol as TableExasol;
use Keboola\InputMapping\Table\Strategy\Local as TableLocal;
use Keboola\InputMapping\Table\Strategy\Redshift as TableRedshift;
use Keboola\InputMapping\Table\Strategy\S3 as TableS3;
use Keboola\InputMapping\Table\Strategy\Snowflake as TableSnowflake;
use Keboola\InputMapping\Table\Strategy\Synapse as TableSynapse;
use Keboola\InputMapping\Table\Strategy\Teradata as TableTeradata;
use Keboola\InputMapping\Table\StrategyInterface as TableStrategyInterface;

class StrategyFactory extends AbstractStrategyFactory
{
    /** @var InputMappingStagingDefinition[] */
    protected array $strategyMap = [];

    /**
     * @return InputMappingStagingDefinition[]
     */
    public function getStrategyMap(): array
    {
        if (empty($this->strategyMap)) {
            $this->strategyMap = [
                self::ABS => new InputMappingStagingDefinition(
                    self::ABS,
                    FileLocal::class,
                    TableABS::class,
                ),
                self::LOCAL => new InputMappingStagingDefinition(
                    self::LOCAL,
                    FileLocal::class,
                    TableLocal::class,
                ),
                self::S3 => new InputMappingStagingDefinition(
                    self::S3,
                    FileLocal::class,
                    TableS3::class,
                ),
                self::WORKSPACE_ABS => new InputMappingStagingDefinition(
                    self::WORKSPACE_ABS,
                    FileABSWorkspace::class,
                    TableABSWorkspace::class,
                ),
                self::WORKSPACE_REDSHIFT => new InputMappingStagingDefinition(
                    self::WORKSPACE_REDSHIFT,
                    FileLocal::class,
                    TableRedshift::class,
                ),
                self::WORKSPACE_SNOWFLAKE => new InputMappingStagingDefinition(
                    self::WORKSPACE_SNOWFLAKE,
                    FileLocal::class,
                    TableSnowflake::class,
                ),
                self::WORKSPACE_SYNAPSE => new InputMappingStagingDefinition(
                    self::WORKSPACE_SYNAPSE,
                    FileLocal::class,
                    TableSynapse::class,
                ),
                self::WORKSPACE_EXASOL => new InputMappingStagingDefinition(
                    self::WORKSPACE_EXASOL,
                    FileLocal::class,
                    TableExasol::class,
                ),
                self::WORKSPACE_TERADATA => new InputMappingStagingDefinition(
                    self::WORKSPACE_TERADATA,
                    FileLocal::class,
                    TableTeradata::class,
                ),
                self::WORKSPACE_BIGQUERY => new InputMappingStagingDefinition(
                    self::WORKSPACE_BIGQUERY,
                    FileLocal::class,
                    TableBigQuery::class,
                ),
            ];
        }
        return $this->strategyMap;
    }

    protected function getStagingDefinition(string $stagingType): InputMappingStagingDefinition
    {
        if (!isset($this->getStrategyMap()[$stagingType])) {
            throw new InvalidInputException(
                sprintf(
                    'Input mapping on type "%s" is not supported. Supported types are "%s".',
                    $stagingType,
                    implode(', ', array_keys($this->getStrategyMap())),
                ),
            );
        }
        return $this->getStrategyMap()[$stagingType];
    }

    public function getFileInputStrategy(string $stagingType, InputFileStateList $fileStateList): FileStrategyInterface
    {
        $stagingDefinition = $this->getStagingDefinition($stagingType);
        try {
            $stagingDefinition->validateFor(AbstractStagingDefinition::STAGING_FILE);
        } catch (StagingException $e) {
            throw new InvalidInputException(
                sprintf('The project does not support "%s" file input backend.', $stagingDefinition->getName()),
                0,
                $e,
            );
        }
        $this->getLogger()->info(sprintf('Using "%s" file input staging.', $stagingDefinition->getName()));
        $className = $stagingDefinition->getFileStagingClass();
        return new $className(
            $this->clientWrapper,
            $this->logger,
            $stagingDefinition->getFileDataProvider(),
            $stagingDefinition->getFileMetadataProvider(),
            $fileStateList,
            $this->format,
        );
    }

    public function getTableInputStrategy(
        string $stagingType,
        string $destination,
        InputTableStateList $tablesState,
    ): TableStrategyInterface {
        $stagingDefinition = $this->getStagingDefinition($stagingType);
        try {
            $stagingDefinition->validateFor(AbstractStagingDefinition::STAGING_TABLE);
        } catch (StagingException $e) {
            throw new InvalidInputException(
                sprintf('The project does not support "%s" table input backend.', $stagingDefinition->getName()),
                0,
                $e,
            );
        }
        $this->getLogger()->info(sprintf('Using "%s" table input staging.', $stagingDefinition->getName()));
        $className = $stagingDefinition->getTableStagingClass();
        return new $className(
            $this->clientWrapper,
            $this->logger,
            $stagingDefinition->getTableDataProvider(),
            $stagingDefinition->getTableMetadataProvider(),
            $tablesState,
            $destination,
            $this->format,
        );
    }
}
