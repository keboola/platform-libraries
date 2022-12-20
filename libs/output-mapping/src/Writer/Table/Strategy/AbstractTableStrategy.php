<?php

namespace Keboola\OutputMapping\Writer\Table\Strategy;

use Keboola\InputMapping\Staging\ProviderInterface;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Writer\Table\MappingSource;
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

    /** @var boolean */
    protected $isFailedJob;

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
        $format,
        $isFailedJob = false
    ) {
        $this->clientWrapper = $storageClient;
        $this->logger = $logger;
        $this->dataStorage = $dataStorage;
        $this->metadataStorage = $metadataStorage;
        $this->format = $format;
        $this->isFailedJob = $isFailedJob;
    }

    public function getDataStorage()
    {
        return $this->dataStorage;
    }

    public function getMetadataStorage()
    {
        return $this->metadataStorage;
    }

    /**
     * @param MappingSource[] $mappingSources
     * @param array<array{source: string}> $mappings
     * @return MappingSource[]
     */
    protected function combineSourcesWithMappingsFromConfiguration(array $mappingSources, array $mappings)
    {
        $mappingsBySource = [];
        foreach ($mappings as $mapping) {
            $mappingsBySource[$mapping['source']][] = $mapping;
        }

        $sourcesWithMapping = [];
        foreach ($mappingSources as $source) {
            $sourceName = $source->getSourceName();

            $sourceMappings = isset($mappingsBySource[$sourceName]) ? $mappingsBySource[$sourceName] : [];
            unset($mappingsBySource[$sourceName]);

            if (count($sourceMappings) === 0) {
                $sourcesWithMapping[] = $source;
                continue;
            }

            foreach ($sourceMappings as $sourceMapping) {
                $sourceCopy = clone $source;
                $sourceCopy->setMapping($sourceMapping);
                $sourcesWithMapping[] = $sourceCopy;
            }
        }

        if (count($mappingsBySource) > 0) {
            $invalidSources = array_keys($mappingsBySource);
            $invalidSources = array_map(function ($source) {
                return sprintf('"%s"', $source);
            }, $invalidSources);

            // we don't care about missing sources if the job is failed
            // well, we probably should care about missing write-always sources :-/
            if (!$this->isFailedJob) {
                throw new InvalidOutputException(
                    sprintf('Table sources not found: %s', implode(', ', $invalidSources)),
                    404
                );
            }
        }

        return $sourcesWithMapping;
    }
}
