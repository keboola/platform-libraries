<?php

namespace Keboola\OutputMapping\Writer\File;

use Keboola\InputMapping\Reader\Reader;
use Keboola\InputMapping\Reader\WorkspaceProviderInterface;
use Keboola\OutputMapping\Exception\OutputOperationException;
use Keboola\OutputMapping\Writer\File\Strategy\ABSWorkspace;
use Keboola\OutputMapping\Writer\File\Strategy\Local;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\LoggerInterface;

class StrategyFactory
{
    /** @var ClientWrapper */
    private $clientWrapper;

    /** @var WorkspaceProviderInterface */
    private $workspaceProvider;

    /** @var LoggerInterface */
    private $logger;

    /** @var string */
    private $format;

    /** @var string[] */
    private $strategyMap = [
        Reader::STAGING_S3 => Local::class,
        Reader::STAGING_ABS => Local::class,
        Reader::STAGING_REDSHIFT => Local::class,
        Reader::STAGING_SNOWFLAKE => Local::class,
        Reader::STAGING_SYNAPSE => Local::class,
        Reader::STAGING_LOCAL => Local::class,
        Reader::STAGING_ABS_WORKSPACE => ABSWorkspace::class,
    ];

    public function __construct(
        ClientWrapper $clientWrapper,
        LoggerInterface $logger,
        WorkspaceProviderInterface $workspaceProvider,
        $format
    ) {
        $this->clientWrapper = $clientWrapper;
        $this->logger = $logger;
        $this->workspaceProvider = $workspaceProvider;
        $this->format = $format;
    }

    /**
     * @param string $storageType
     * @return StrategyInterface
     */
    public function getStrategy($storageType)
    {
        if (!isset($this->strategyMap[$storageType])) {
            throw new OutputOperationException(
                'FilesStrategy parameter "storageType" must be one of: ' .
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
            $this->format
        );
    }
}
