<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Table;

use Keboola\OutputMapping\Configuration\Table\Manifest;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Mapping\MappingFromRawConfigurationAndPhysicalDataWithManifest;
use Psr\Log\LoggerInterface;
use SplFileInfo;

class SlicerDecider
{

    public function __construct(readonly private LoggerInterface $logger)
    {
    }

    /**
     * @param MappingFromRawConfigurationAndPhysicalDataWithManifest[] $combinedSources
     */
    public function decideSliceFiles(array $combinedSources): array
    {
        $filesForSlicing = [];
        $sourceOccurrences = [];
        foreach ($combinedSources as $combinedSource) {
            $occurrences = ($sourceOccurrences[$combinedSource->getSourceName()] ?? 0) + 1;
            $sourceOccurrences[$combinedSource->getSourceName()] = $occurrences;
        }

        foreach ($combinedSources as $combinedSource) {
            if ($sourceOccurrences[$combinedSource->getSourceName()] > 1) {
                throw new InvalidOutputException(sprintf(
                    'Source "%s" has multiple destinations set.',
                    $combinedSource->getSourceName(),
                ));
            }

            if ($this->decideSliceFile($combinedSource)) {
                $filesForSlicing[] = $combinedSource;
            }
        }
        return $filesForSlicing;
    }

    private function decideSliceFile(MappingFromRawConfigurationAndPhysicalDataWithManifest $combinedSource): bool
    {
        if ($combinedSource->isSliced() && !$combinedSource->getManifest()) {
            $this->logger->warning(sprintf(
                'Sliced files without manifest are not supported. Skipping file "%s"',
                $combinedSource->getSourceName(),

            ));
            return false;
        }

        $sourceFile = new SplFileInfo($combinedSource->getPathName());
        if (!$sourceFile->getSize()) {
            $this->logger->warning(sprintf(
                'Empty files cannot be sliced. Skipping file "%s".',
                $combinedSource->getSourceName(),
            ));
            return false;
        }

        $mapping = $combinedSource->getConfiguration();
        if (!$mapping) {
            return true;
        }
        $hasNonDefaultDelimiter = $mapping->getDelimiter() !== Manifest::DEFAULT_DELIMITER;
        $hasNonDefaultEnclosure = $mapping->getEnclosure() !== Manifest::DEFAULT_ENCLOSURE;
        $hasColumns = $mapping->getColumns();

        if ($hasNonDefaultDelimiter || $hasNonDefaultEnclosure || $hasColumns) {
            // TODO - https://keboola.atlassian.net/browse/PST-1193
            throw new InvalidOutputException(sprintf(
                'Params "delimiter", "enclosure" or "columns" specified in mapping are not longer supported.' .
                ' Skipping file "%s".',
                $combinedSource->getSourceName(),
            ));
        }

        return true;
    }
}
