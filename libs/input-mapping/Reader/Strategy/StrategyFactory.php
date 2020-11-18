<?php

namespace Keboola\InputMapping\Reader\Strategy;

use Keboola\InputMapping\Exception\InvalidInputException;
use Keboola\InputMapping\Reader\Reader;
use Keboola\InputMapping\Reader\State\InputTableStateList;
use Keboola\InputMapping\Reader\WorkspaceProviderInterface;
use Keboola\StorageApi\Client;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\LoggerInterface;

class StrategyFactory
{
    /** @var ClientWrapper */
    private $clientWrapper;

    /** @var WorkspaceProviderInterface */
    private $workspaceProvider;

    /** LoggerInterface */
    private $logger;

    /** @var InputTableStateList */
    protected $tablesState;

    /** string */
    protected $destination;

    private $strategyMap = [
        Reader::STAGING_S3 => S3Strategy::class,
        Reader::STAGING_ABS => ABSStrategy::class,
        Reader::STAGING_LOCAL => LocalStrategy::class,
        Reader::STAGING_REDSHIFT => RedshiftStrategy::class,
        Reader::STAGING_SNOWFLAKE => SnowflakeStrategy::class,
        Reader::STAGING_SYNAPSE => SynapseStrategy::class,
        Reader::STAGING_ABS_WORKSPACE => ABSWorkspaceStrategy::class,
    ];

    /** @var string */
    private $format;

    public function __construct(
        ClientWrapper $clientWrapper,
        LoggerInterface $logger,
        WorkspaceProviderInterface $workspaceProvider,
        InputTableStateList $tablesState,
        $destination,
        $format = 'json'
    ) {
        $this->clientWrapper = $clientWrapper;
        $this->logger = $logger;
        $this->workspaceProvider = $workspaceProvider;
        $this->tablesState = $tablesState;
        $this->destination = $destination;
        $this->format = $format;
    }

    /**
     * @param string $storageType
     * @return StrategyInterface
     */
    public function getStrategy($storageType)
    {
        if (!isset($this->strategyMap[$storageType])) {
            throw new InvalidInputException(
                'Parameter "storage" must be one of: ' .
                implode(
                    ', ',
                    array_keys($this->strategyMap)
                )
            );
        }

        $className = $this->strategyMap[$storageType];

        return new $className(
            $this->clientWrapper,
            $this->logger,
            $this->workspaceProvider,
            $this->tablesState,
            $this->destination,
            $this->format
        );
    }
}
