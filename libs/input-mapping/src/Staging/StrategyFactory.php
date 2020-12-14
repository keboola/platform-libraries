<?php

namespace Keboola\InputMapping\Staging;

use Keboola\InputMapping\Exception\InputOperationException;
use Keboola\InputMapping\Exception\StagingException;
use Keboola\InputMapping\File\Strategy\ABSWorkspace as FileABSWorkspace;
use Keboola\InputMapping\File\Strategy\Local as FileLocal;
use Keboola\InputMapping\File\StrategyInterface as FileStrategyInterface;
use Keboola\InputMapping\State\InputTableStateList;
use Keboola\InputMapping\Table\Strategy\ABS as TableABS;
use Keboola\InputMapping\Table\Strategy\ABSWorkspace as TableABSWorkspace;
use Keboola\InputMapping\Table\Strategy\Local as TableLocal;
use Keboola\InputMapping\Table\Strategy\Redshift as TableRedshift;
use Keboola\InputMapping\Table\Strategy\S3 as TableS3;
use Keboola\InputMapping\Table\Strategy\Snowflake as TableSnowflake;
use Keboola\InputMapping\Table\Strategy\Synapse as TableSynapse;
use Keboola\InputMapping\Table\StrategyInterface as TableStrategyInterface;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\LoggerInterface;

class StrategyFactory
{
    const ABS = 'abs';
    const LOCAL = 'local';
    const S3 = 's3';
    const WORKSPACE_ABS = 'workspace-abs';
    const WORKSPACE_REDSHIFT = 'workspace-redshift';
    const WORKSPACE_SNOWFLAKE = 'workspace-snowflake';
    const WORKSPACE_SYNAPSE = 'workspace-synapse';

    /** @var Definition[] */
    private $strategyMap;

    /** @var ClientWrapper */
    private $clientWrapper;

    /** LoggerInterface */
    private $logger;

    /** @var string */
    private $format;

    /**
     * StagingStrategyFactory constructor.
     * @param ClientWrapper $clientWrapper
     * @param LoggerInterface $logger
     * @param string $format
     */
    public function __construct(ClientWrapper $clientWrapper, LoggerInterface $logger, $format)
    {
        $this->clientWrapper = $clientWrapper;
        $this->logger = $logger;
        $this->format = $format;
    }

    /**
     * @return Definition[]
     */
    public function getStrategyMap()
    {
        if (empty($this->strategyMap)) {
            $this->strategyMap = [
                self::ABS => new Definition(
                    self::ABS,
                    FileLocal::class,
                    TableABS::class
                ),
                self::LOCAL => new Definition(
                    self::LOCAL,
                    FileLocal::class,
                    TableLocal::class
                ),
                self::S3 => new Definition(
                    self::S3,
                    FileLocal::class,
                    TableS3::class
                ),
                self::WORKSPACE_ABS => new Definition(
                    self::WORKSPACE_ABS,
                    FileABSWorkspace::class,
                    TableABSWorkspace::class
                ),
                self::WORKSPACE_REDSHIFT => new Definition(
                    self::WORKSPACE_REDSHIFT,
                    FileLocal::class,
                    TableRedshift::class
                ),
                self::WORKSPACE_SNOWFLAKE => new Definition(
                    self::WORKSPACE_SNOWFLAKE,
                    FileLocal::class,
                    TableSnowflake::class
                ),
                self::WORKSPACE_SYNAPSE => new Definition(
                    self::WORKSPACE_SYNAPSE,
                    FileLocal::class,
                    TableSynapse::class
                ),
            ];
        }
        return $this->strategyMap;
    }

    /**
     * @param CapabilityInterface $capability
     * @param Fulfillment[] $fulfills
     */
    public function addStagingCapability(CapabilityInterface $capability, $fulfills)
    {
        foreach ($fulfills as $stagingType => $fulfillment) {
            if (!isset($this->getStrategyMap()[$stagingType])) {
                throw new StagingException(sprintf(
                    'Staging "%s" is unknown. Known types are "%s".',
                    $stagingType,
                    implode(', ', array_keys($this->getStrategyMap()))
                ));
            }
            $staging = $this->getStrategyMap()[$stagingType];
            foreach ($fulfillment->getFulfillmentTypes() as $fulfillmentType) {
                switch ($fulfillmentType) {
                    case Fulfillment::TABLE_DATA:
                        $staging->setTableDataCapability($capability);
                        break;
                    case Fulfillment::TABLE_METADATA:
                        $staging->setTableMetadataCapability($capability);
                        break;
                    case Fulfillment::FILE_DATA:
                        $staging->setFileDataCapability($capability);
                        break;
                    case Fulfillment::FILE_METADATA:
                        $staging->setFileMetadataCapability($capability);
                        break;
                    default:
                        throw new StagingException(sprintf('Invalid fulfilment type: "%s". ', $fulfillmentType));
                }
            }
        }
    }

    /**
     * @return LoggerInterface
     */
    public function getLogger()
    {
        return $this->logger;
    }

    /**
     * @param string $stagingType
     * @return Definition
     */
    private function getStagingDefinition($stagingType)
    {
        if (!isset($this->getStrategyMap()[$stagingType])) {
            throw new InputOperationException(
                sprintf(
                    'Input mapping on type "%s" is not supported. Supported types are "%s".',
                    $stagingType,
                    implode(', ', array_keys($this->getStrategyMap()))
                )
            );
        }
        return $this->getStrategyMap()[$stagingType];
    }

    /**
     * @param string $stagingType
     * @return FileStrategyInterface
     */
    public function getFileStrategy($stagingType)
    {
        $stagingDefinition = $this->getStagingDefinition($stagingType);
        $stagingDefinition->validateFor(Definition::STAGING_FILE);
        $this->getLogger()->info(sprintf('Using "%s" file staging.', $stagingDefinition->getName()));
        $className = $stagingDefinition->getFileStagingClass();
        return new $className(
            $this->clientWrapper,
            $this->logger,
            $stagingDefinition->getFileDataCapability(),
            $stagingDefinition->getFileMetadataCapability(),
            $this->format
        );
    }

    /**
     * @param string $stagingType
     * @param string $destination
     * @param InputTableStateList $tablesState
     * @return TableStrategyInterface
     */
    public function getTableStrategy($stagingType, $destination, InputTableStateList $tablesState)
    {
        $stagingDefinition = $this->getStagingDefinition($stagingType);
        $stagingDefinition->validateFor(Definition::STAGING_TABLE);
        $this->getLogger()->info(sprintf('Using "%s" table staging.', $stagingDefinition->getName()));
        $className = $stagingDefinition->getTableStagingClass();
        return new $className(
            $this->clientWrapper,
            $this->logger,
            $tablesState,
            $destination,
            $stagingDefinition->getTableDataCapability(),
            $stagingDefinition->getTableMetadataCapability(),
            $this->format
        );
    }
}
