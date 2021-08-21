<?php

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
use Keboola\InputMapping\Table\Strategy\Local as TableLocal;
use Keboola\InputMapping\Table\Strategy\Redshift as TableRedshift;
use Keboola\InputMapping\Table\Strategy\S3 as TableS3;
use Keboola\InputMapping\Table\Strategy\Snowflake as TableSnowflake;
use Keboola\InputMapping\Table\Strategy\Synapse as TableSynapse;
use Keboola\InputMapping\Table\Strategy\Exasol as TableExasol;
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
    const WORKSPACE_EXASOL = 'workspace-exasol';

    /** @var Definition[] */
    protected $strategyMap;

    /** @var ClientWrapper */
    protected $clientWrapper;

    /** LoggerInterface */
    protected $logger;

    /** @var string */
    protected $format;

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
                self::WORKSPACE_EXASOL => new Definition(
                    self::WORKSPACE_EXASOL,
                    FileLocal::class,
                    TableExasol::class
                ),
            ];
        }
        return $this->strategyMap;
    }

    /**
     * @param ProviderInterface $provider
     * @param Scope[] $scopes
     */
    public function addProvider(ProviderInterface $provider, $scopes)
    {
        foreach ($scopes as $stagingType => $scope) {
            if (!isset($this->getStrategyMap()[$stagingType])) {
                throw new StagingException(sprintf(
                    'Staging "%s" is unknown. Known types are "%s".',
                    $stagingType,
                    implode(', ', array_keys($this->getStrategyMap()))
                ));
            }
            $staging = $this->getStrategyMap()[$stagingType];
            foreach ($scope->getScopeTypes() as $scopeType) {
                switch ($scopeType) {
                    case Scope::TABLE_DATA:
                        $staging->setTableDataProvider($provider);
                        break;
                    case Scope::TABLE_METADATA:
                        $staging->setTableMetadataProvider($provider);
                        break;
                    case Scope::FILE_DATA:
                        $staging->setFileDataProvider($provider);
                        break;
                    case Scope::FILE_METADATA:
                        $staging->setFileMetadataProvider($provider);
                        break;
                    default:
                        throw new StagingException(sprintf('Invalid scope type: "%s". ', $scopeType));
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
     * @return ClientWrapper
     */
    public function getClientWrapper()
    {
        return $this->clientWrapper;
    }

    /**
     * @param string $stagingType
     * @return Definition
     */
    protected function getStagingDefinition($stagingType)
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

    /**
     * @param string $stagingType
     * @param InputFileStateList $fileStateList
     * @return FileStrategyInterface
     */
    public function getFileInputStrategy($stagingType, InputFileStateList $fileStateList)
    {
        $stagingDefinition = $this->getStagingDefinition($stagingType);
        try {
            $stagingDefinition->validateFor(Definition::STAGING_FILE);
        } catch (StagingException $e) {
            throw new InvalidInputException(
                sprintf('The project does not support "%s" file input backend.', $stagingDefinition->getName()),
                0,
                $e
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
            $this->format
        );
    }

    /**
     * @param string $stagingType
     * @param string $destination
     * @param InputTableStateList $tablesState
     * @return TableStrategyInterface
     */
    public function getTableInputStrategy($stagingType, $destination, InputTableStateList $tablesState)
    {
        $stagingDefinition = $this->getStagingDefinition($stagingType);
        try {
            $stagingDefinition->validateFor(Definition::STAGING_TABLE);
        } catch (StagingException $e) {
            throw new InvalidInputException(
                sprintf('The project does not support "%s" table input backend.', $stagingDefinition->getName()),
                0,
                $e
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
            $this->format
        );
    }
}
