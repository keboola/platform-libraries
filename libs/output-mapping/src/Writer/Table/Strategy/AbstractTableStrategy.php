<?php

namespace Keboola\OutputMapping\Writer\Table\Strategy;

use Keboola\InputMapping\Staging\ProviderInterface;
use Keboola\OutputMapping\Writer\Table\StrategyInterface;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\LoggerInterface;

abstract class AbstractTableStrategy implements StrategyInterface
{
    /** @var ClientWrapper */
    protected $clientWrapper;

    /** @var LoggerInterface */
    protected $logger;

    /** @var ProviderInterface */
    protected $dataStorage;

    /** @var ProviderInterface */
    protected $metadataStorage;

    /** @var string */
    protected $format;

    /**
     * @param ClientWrapper $storageClient
     * @param LoggerInterface $logger
     * @param ProviderInterface $dataStorage
     * @param ProviderInterface $metadataStorage
     * @param string $format
     */
    public function __construct(
        ClientWrapper $storageClient,
        LoggerInterface $logger,
        ProviderInterface $dataStorage,
        ProviderInterface $metadataStorage,
        $format
    ) {
        $this->clientWrapper = $storageClient;
        $this->logger = $logger;
        $this->dataStorage = $dataStorage;
        $this->metadataStorage = $metadataStorage;
        $this->format = $format;
    }

    public function getDataStorage()
    {
        return $this->dataStorage;
    }

    public function getMetadataStorage()
    {
        return $this->metadataStorage;
    }
}
