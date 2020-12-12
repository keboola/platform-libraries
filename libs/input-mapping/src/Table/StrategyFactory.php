<?php

namespace Keboola\InputMapping\Table;

use Keboola\InputMapping\Exception\InvalidInputException;
use Keboola\InputMapping\Reader;
use Keboola\InputMapping\State\InputTableStateList;
use Keboola\InputMapping\Table\Strategy\ABS;
use Keboola\InputMapping\Table\Strategy\ABSWorkspace;
use Keboola\InputMapping\Table\Strategy\Local;
use Keboola\InputMapping\Table\Strategy\Redshift;
use Keboola\InputMapping\Table\Strategy\S3;
use Keboola\InputMapping\Table\Strategy\Snowflake;
use Keboola\InputMapping\Table\Strategy\Synapse;
use Keboola\InputMapping\WorkspaceProviderInterface;
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
        Reader::STAGING_S3 => S3::class,
        Reader::STAGING_ABS => ABS::class,
        Reader::STAGING_LOCAL => Local::class,
        Reader::STAGING_REDSHIFT => Redshift::class,
        Reader::STAGING_SNOWFLAKE => Snowflake::class,
        Reader::STAGING_SYNAPSE => Synapse::class,
        Reader::STAGING_ABS_WORKSPACE => ABSWorkspace::class,
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
