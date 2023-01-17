<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Staging;

use Keboola\InputMapping\Exception\InvalidInputException;
use Keboola\InputMapping\Exception\StagingException;
use Keboola\InputMapping\Staging\AbstractDefinition;
use Keboola\InputMapping\Staging\AbstractStrategyFactory;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Writer\AbstractWriter;
use Keboola\OutputMapping\Writer\File\Strategy\ABSWorkspace;
use Keboola\OutputMapping\Writer\File\Strategy\Local;
use Keboola\OutputMapping\Writer\File\StrategyInterface as FileStrategyInterface;
use Keboola\OutputMapping\Writer\Table\Strategy\AbsWorkspaceTableStrategy;
use Keboola\OutputMapping\Writer\Table\Strategy\LocalTableStrategy;
use Keboola\OutputMapping\Writer\Table\Strategy\SqlWorkspaceTableStrategy;
use Keboola\OutputMapping\Writer\Table\StrategyInterface as TableStrategyInterface;
use Keboola\OutputMapping\Writer\TableWriter;

class StrategyFactory extends AbstractStrategyFactory
{
    /** @var Definition[] */
    protected array $strategyMap = [];

    /**
     * @return Definition[]
     */
    public function getStrategyMap(): array
    {
        if (empty($this->strategyMap)) {
            $this->strategyMap = [
                self::LOCAL => new Definition(
                    self::LOCAL,
                    Local::class,
                    LocalTableStrategy::class
                ),
                self::WORKSPACE_ABS => new Definition(
                    self::WORKSPACE_ABS,
                    ABSWorkspace::class,
                    AbsWorkspaceTableStrategy::class
                ),
                self::WORKSPACE_REDSHIFT => new Definition(
                    self::WORKSPACE_REDSHIFT,
                    Local::class,
                    SqlWorkspaceTableStrategy::class
                ),
                self::WORKSPACE_SNOWFLAKE => new Definition(
                    self::WORKSPACE_SNOWFLAKE,
                    Local::class,
                    SqlWorkspaceTableStrategy::class
                ),
                self::WORKSPACE_SYNAPSE => new Definition(
                    self::WORKSPACE_SYNAPSE,
                    Local::class,
                    SqlWorkspaceTableStrategy::class
                ),
                self::WORKSPACE_EXASOL => new Definition(
                    self::WORKSPACE_EXASOL,
                    Local::class,
                    SqlWorkspaceTableStrategy::class
                ),
                self::WORKSPACE_TERADATA => new Definition(
                    self::WORKSPACE_TERADATA,
                    Local::class,
                    SqlWorkspaceTableStrategy::class
                ),
                self::WORKSPACE_BIGQUERY => new Definition(
                    self::WORKSPACE_BIGQUERY,
                    Local::class,
                    SqlWorkspaceTableStrategy::class
                ),
            ];
        }
        return $this->strategyMap;
    }

    protected function getStagingDefinition(string $stagingType): Definition
    {
        if (!isset($this->getStrategyMap()[$stagingType])) {
            throw new InvalidInputException(
                sprintf(
                    'Input mapping on type "%s" is not supported. Supported types are "%s".',
                    $stagingType,
                    implode(', ', array_keys($this->getStrategyMap()))
                )
            );
        }
        return $this->getStrategyMap()[$stagingType];
    }

    public function getFileOutputStrategy(string $stagingType): FileStrategyInterface
    {
        $stagingDefinition = $this->getStagingDefinition($stagingType);
        try {
            $stagingDefinition->validateFor(AbstractDefinition::STAGING_FILE);
        } catch (StagingException $e) {
            throw new InvalidOutputException(
                sprintf('The project does not support "%s" file output backend.', $stagingDefinition->getName()),
                0,
                $e
            );
        }
        $this->getLogger()->info(sprintf('Using "%s" file output staging.', $stagingDefinition->getName()));
        $className = $stagingDefinition->getFileStagingClass();
        return new $className(
            $this->clientWrapper,
            $this->logger,
            $stagingDefinition->getFileDataProvider(),
            $stagingDefinition->getFileMetadataProvider(),
            $this->format
        );
    }

    public function getTableOutputStrategy(string $stagingType, bool $isFailedJob = false): TableStrategyInterface
    {
        $stagingDefinition = $this->getStagingDefinition($stagingType);
        try {
            $stagingDefinition->validateFor(AbstractDefinition::STAGING_TABLE);
        } catch (StagingException $e) {
            throw new InvalidOutputException(
                sprintf('The project does not support "%s" table output backend.', $stagingDefinition->getName()),
                0,
                $e
            );
        }
        $this->getLogger()->info(sprintf('Using "%s" table output staging.', $stagingDefinition->getName()));
        $className = $stagingDefinition->getTableStagingClass();
        return new $className(
            $this->clientWrapper,
            $this->logger,
            $stagingDefinition->getTableDataProvider(),
            $stagingDefinition->getTableMetadataProvider(),
            $this->format,
            $isFailedJob
        );
    }
}
