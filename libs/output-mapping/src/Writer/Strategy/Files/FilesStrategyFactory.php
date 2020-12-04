<?php


namespace Keboola\OutputMapping\Writer\Strategy\Files;


use Keboola\InputMapping\Exception\InvalidInputException;
use Keboola\InputMapping\Reader\Reader;
use Keboola\InputMapping\Reader\WorkspaceProviderInterface;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\LoggerInterface;

class FilesStrategyFactory
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
        Reader::STAGING_S3 => LocalFilesStrategy::class,
        Reader::STAGING_ABS => LocalFilesStrategy::class,
        Reader::STAGING_REDSHIFT => LocalFilesStrategy::class,
        Reader::STAGING_SNOWFLAKE => LocalFilesStrategy::class,
        Reader::STAGING_SYNAPSE => LocalFilesStrategy::class,
        Reader::STAGING_LOCAL => LocalFilesStrategy::class,
        Reader::STAGING_ABS_WORKSPACE => ABSWorkspaceFilesStrategy::class,
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
     * @return FilesStrategyInterface
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
