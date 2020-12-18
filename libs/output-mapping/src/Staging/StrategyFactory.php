<?php

namespace Keboola\OutputMapping\Staging;

use Keboola\InputMapping\Exception\StagingException;
use Keboola\InputMapping\Staging\Definition;
use Keboola\InputMapping\Staging\StrategyFactory as InputMappingStrategyFactory;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Writer\File\Strategy\ABSWorkspace;
use Keboola\OutputMapping\Writer\File\Strategy\Local;
use Keboola\OutputMapping\Writer\File\StrategyInterface as FileStrategyInterface;
use Keboola\OutputMapping\Writer\Table\Strategy\AllEncompassingTableStrategy;
use Keboola\OutputMapping\Writer\Table\StrategyInterface as TableStrategyInterface;

class StrategyFactory extends InputMappingStrategyFactory
{
    /**
     * @return Definition[]
     */
    public function getStrategyMap()
    {
        if (empty($this->strategyMap)) {
            $this->strategyMap = [
                self::LOCAL => new Definition(
                    self::LOCAL,
                    Local::class,
                    AllEncompassingTableStrategy::class
                ),
                self::WORKSPACE_ABS => new Definition(
                    self::WORKSPACE_ABS,
                    ABSWorkspace::class,
                    AllEncompassingTableStrategy::class
                ),
                self::WORKSPACE_REDSHIFT => new Definition(
                    self::WORKSPACE_REDSHIFT,
                    Local::class,
                    AllEncompassingTableStrategy::class
                ),
                self::WORKSPACE_SNOWFLAKE => new Definition(
                    self::WORKSPACE_SNOWFLAKE,
                    Local::class,
                    AllEncompassingTableStrategy::class
                ),
                self::WORKSPACE_SYNAPSE => new Definition(
                    self::WORKSPACE_SYNAPSE,
                    Local::class,
                    AllEncompassingTableStrategy::class
                ),
            ];
        }
        return $this->strategyMap;
    }

    /**
     * @param string $stagingType
     * @return FileStrategyInterface
     */
    public function getFileOutputStrategy($stagingType)
    {
        $stagingDefinition = $this->getStagingDefinition($stagingType);
        try {
            $stagingDefinition->validateFor(Definition::STAGING_FILE);
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

    /**
     * @param string $stagingType
     * @return TableStrategyInterface
     */
    public function getTableOutputStrategy($stagingType)
    {
        $stagingDefinition = $this->getStagingDefinition($stagingType);
        try {
            $stagingDefinition->validateFor(Definition::STAGING_TABLE);
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
            $this->format
        );
    }
}
