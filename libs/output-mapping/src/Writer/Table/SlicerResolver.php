<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Table;

use Keboola\OutputMapping\Configuration\Table\Manifest;
use Keboola\OutputMapping\Mapping\MappingFromRawConfigurationAndPhysicalDataWithManifest;
use Psr\Log\LoggerInterface;
use SplFileInfo;

class SlicerResolver
{

    public function __construct(readonly private LoggerInterface $logger)
    {
    }

    /**
     * @param MappingFromRawConfigurationAndPhysicalDataWithManifest[] $combinedSources
     */
    public function resolveSliceFiles(array $combinedSources): array
    {
        $filesForSlicing = [];
        $sourceOccurrences = [];
        foreach ($combinedSources as $combinedSource) {
            $occurrences = ($sourceOccurrences[$combinedSource->getSourceName()] ?? 0) + 1;
            $sourceOccurrences[$combinedSource->getSourceName()] = $occurrences;
        }

        foreach ($combinedSources as $combinedSource) {
            if ($sourceOccurrences[$combinedSource->getSourceName()] > 1) {
                $this->logger->warning(sprintf(
                    'Source "%s" has multiple destinations set.',
                    $combinedSource->getSourceName(),
                ));
                continue;
            }

            if ($this->resolveSliceFile($combinedSource)) {
                $filesForSlicing[] = $combinedSource;
            }
            
        }
        return $filesForSlicing;
    }

    private function resolveSliceFile(MappingFromRawConfigurationAndPhysicalDataWithManifest $combinedSource): bool
    {
        if ($combinedSource->isSliced() && !$combinedSource->getManifest()) {
            $this->logger->warning('Sliced files without manifest are not supported.');
            return false;
        }

        $sourceFile = new SplFileInfo($combinedSource->getPathName());
        if (!$sourceFile->getSize()) {
            $this->logger->warning('Empty files cannot be sliced.');
            return false;
        }

        $mapping = $combinedSource->getConfiguration();
        $hasNonDefaultDelimiter = $mapping->getDelimiter() !== Manifest::DEFAULT_DELIMITER;
        $hasNonDefaultEnclosure = $mapping->getEnclosure() !== Manifest::DEFAULT_ENCLOSURE;
        $hasColumns = $mapping->getColumns() !== [];

        if ($hasNonDefaultDelimiter || $hasNonDefaultEnclosure || $hasColumns) {
            $this->logger->warning('Params "delimiter", "enclosure" or "columns" specified in mapping are not longer supported.');
            return false;
        }

        return true;
    }
}