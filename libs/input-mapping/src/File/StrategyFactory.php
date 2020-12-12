<?php

namespace Keboola\InputMapping\File;

use Keboola\InputMapping\Exception\InvalidInputException;
use Keboola\InputMapping\File\Strategy\ABSWorkspace;
use Keboola\InputMapping\File\Strategy\Local;
use Keboola\InputMapping\Reader;
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

    /** string */
    protected $destination;

    private $strategyMap = [
        Reader::STAGING_S3 => Local::class,
        Reader::STAGING_ABS => Local::class,
        Reader::STAGING_REDSHIFT => Local::class,
        Reader::STAGING_SNOWFLAKE => Local::class,
        Reader::STAGING_SYNAPSE => Local::class,
        Reader::STAGING_LOCAL => Local::class,
        Reader::STAGING_ABS_WORKSPACE => ABSWorkspace::class,
    ];

    /** @var string */
    private $format;

    public function __construct(
        ClientWrapper $clientWrapper,
        LoggerInterface $logger,
        WorkspaceProviderInterface $workspaceProvider,
        $destination,
        $format = 'json'
    ) {
        $this->clientWrapper = $clientWrapper;
        $this->logger = $logger;
        $this->workspaceProvider = $workspaceProvider;
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
                'FilesStrategy parameter "storage" must be one of: ' .
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
            $this->destination,
            $this->format
        );
    }
}
