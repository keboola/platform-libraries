<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Table\MappingResolver;

use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Writer\Table\MappingSource;

abstract class AbstractMappingResolver implements MappingResolverInterface
{
    /**
     * @param MappingSource[] $mappingSources
     * @param array<array{source: string}> $mappings
     * @return MappingSource[]
     */
    protected function combineSourcesWithMappingsFromConfiguration(
        array $mappingSources,
        array $mappings,
        bool $isFailedJob,
    ): array {
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
            if (!$isFailedJob) {
                throw new InvalidOutputException(
                    sprintf('Table sources not found: %s', implode(', ', $invalidSources)),
                    404,
                );
            }
        }

        return $sourcesWithMapping;
    }
}
