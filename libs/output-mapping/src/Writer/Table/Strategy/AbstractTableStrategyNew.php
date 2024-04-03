<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Table\Strategy;

use Keboola\InputMapping\Staging\ProviderInterface;
use Keboola\OutputMapping\Writer\Table\StrategyInterface;
use Keboola\OutputMapping\Writer\Table\StrategyInterfaceNew;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\LoggerInterface;

abstract class AbstractTableStrategyNew implements StrategyInterfaceNew
{
    public function __construct(
        protected readonly ClientWrapper $clientWrapper,
        protected readonly LoggerInterface $logger,
        protected readonly ProviderInterface $dataStorage,
        protected readonly ProviderInterface $metadataStorage,
        protected readonly string $format,
        protected readonly bool $isFailedJob = false,
    ) {
    }

    public function getDataStorage(): ProviderInterface
    {
        return $this->dataStorage;
    }

    public function getMetadataStorage(): ProviderInterface
    {
        return $this->metadataStorage;
    }
}
