<?php

namespace Keboola\OutputMapping\Writer\File\Strategy;

use Keboola\InputMapping\Staging\ProviderInterface;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\LoggerInterface;

class AbstractFileStrategy
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

    /**
     * @param array $storageConfig
     * @return array
     */
    protected function preProcessStorageConfig(array $storageConfig)
    {
        if (!isset($storageConfig['tags'])) {
            $storageConfig['tags'] = [];
        }
        if (!isset($storageConfig['is_permanent'])) {
            $storageConfig['is_permanent'] = false;
        }
        if (!isset($storageConfig['is_encrypted'])) {
            $storageConfig['is_encrypted'] = true;
        }
        if (!isset($storageConfig['is_public'])) {
            $storageConfig['is_public'] = false;
        }
        if (!isset($storageConfig['notify'])) {
            $storageConfig['notify'] = false;
        }
        return $storageConfig;
    }
}
