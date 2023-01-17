<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Table\Strategy;

use Keboola\InputMapping\Staging\ProviderInterface;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Writer\Table\MappingSource;
use Keboola\OutputMapping\Writer\Table\StrategyInterface;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\LoggerInterface;

abstract class AbstractTableStrategy implements StrategyInterface
{
    public function __construct(
        protected readonly ClientWrapper $clientWrapper,
        protected readonly LoggerInterface $logger,
        protected readonly ProviderInterface $dataStorage,
        protected readonly ProviderInterface $metadataStorage,
        protected readonly string $format,
        protected readonly bool $isFailedJob = false
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

    /**
     * @param MappingSource[] $mappingSources
     * @param array<array{source: string}> $mappings
     * @return MappingSource[]
     */
    protected function combineSourcesWithMappingsFromConfiguration(array $mappingSources, array $mappings): array
    {
        $mappingsBySource = [];
        foreach ($mappings as $mapping) {
            $mappingsBySource[$mapping['source']][] = $mapping;
        }

        $sourcesWithMapping = [];
        foreach ($mappingSources as $source) {
            $sourceName = $source->getSourceName();

            $sourceMappings = $mappingsBySource[$sourceName] ?? [];
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
