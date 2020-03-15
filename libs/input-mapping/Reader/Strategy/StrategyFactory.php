<?php

namespace Keboola\InputMapping\Reader\Strategy;

use Keboola\InputMapping\Exception\InvalidInputException;
use Keboola\InputMapping\Reader\State\InputTableStateList;
use Keboola\InputMapping\Reader\WorkspaceProviderInterface;
use Keboola\StorageApi\Client;
use Psr\Log\LoggerInterface;

class StrategyFactory
{
    const STAGING_S3 = 's3';
    const STAGING_LOCAL = 'local';
    const STAGING_SNOWFLAKE = 'workspace-snowflake';
    const STAGING_REDSHIFT = 'workspace-redshift';

    /** @var Client */
    private $storageClient;

    /** @var WorkspaceProviderInterface */
    private $workspaceProvider;

    /** LoggerInterface */
    private $logger;

    /** @var InputTableStateList */
    protected $tablesState;

    /** string */
    protected $destination;

    private $strategyMap = [
        self::STAGING_S3 => S3Strategy::class,
        self::STAGING_LOCAL => LocalStrategy::class,
        self::STAGING_REDSHIFT => RedshiftStrategy::class,
        self::STAGING_SNOWFLAKE => SnowflakeStrategy::class
    ];

    /** @var string */
    private $format;

    public function __construct(
        Client $storageClient,
        LoggerInterface $logger,
        WorkspaceProviderInterface $workspaceProvider,
        InputTableStateList $tablesState,
        $destination,
        $format = 'json'
    ) {
        $this->storageClient = $storageClient;
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
                    [self::STAGING_LOCAL, self::STAGING_S3, self::STAGING_SNOWFLAKE, self::STAGING_REDSHIFT]
                )
            );
        }

        $className = $this->strategyMap[$storageType];

        return new $className(
            $this->storageClient,
            $this->logger,
            $this->workspaceProvider,
            $this->tablesState,
            $this->destination,
            $this->format
        );
    }
}
