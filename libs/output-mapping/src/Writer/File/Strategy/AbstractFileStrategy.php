<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\File\Strategy;

use Keboola\InputMapping\Staging\ProviderInterface;
use Keboola\OutputMapping\Configuration\Adapter;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\LoggerInterface;

class AbstractFileStrategy
{
    /**
     * @param Adapter::FORMAT_YAML | Adapter::FORMAT_JSON $format
     */
    public function __construct(
        protected readonly ClientWrapper $clientWrapper,
        protected readonly LoggerInterface $logger,
        protected readonly ProviderInterface $dataStorage,
        protected readonly ProviderInterface $metadataStorage,
        protected readonly string $format
    ) {
    }

    protected function preProcessStorageConfig(array $storageConfig): array
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
