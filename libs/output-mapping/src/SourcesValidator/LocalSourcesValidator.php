<?php

namespace Keboola\OutputMapping\SourcesValidator;

use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Mapping\MappingFromRawConfiguration;
use Keboola\OutputMapping\Writer\FileItem;

class LocalSourcesValidator implements SourcesValidatorInterface
{

    /**
     * @param FileItem[] $dataItems
     * @param FileItem[] $manifests
     * @return void
     */
    public function validatePhysicalFilesWithManifest(array $dataItems, array $manifests): void
    {
        foreach ($manifests as $manifest) {
            $dataFilePresent = false;
            foreach ($dataItems as $dataItem) {
                if ($manifest->getName() === $dataItem->getName() . '.manifest') {
                    $dataFilePresent = true;
                    break;
                }
            }
            if (!$dataFilePresent) {
                $orphanedManifestSourceNames[] = $manifest->getName();
            }
        }
        if (!empty($orphanedManifestSourceNames)) {
            throw new InvalidOutputException(sprintf(
        'Found orphaned table manifest: %s',
                implode(', ', array_map(fn($v) => sprintf('"%s"', $v), $orphanedManifestSourceNames))
            ));
        }
    }

    /**
     * @param FileItem[] $dataItems
     * @param MappingFromRawConfiguration[] $configurationSource
     */
    public function validatePhysicalFilesWithConfiguration(array $dataItems, array $configurationSource, bool $isFailedJob): void
    {
        $invalidSources = [];
        foreach ($configurationSource as $source) {
            $sourceFound = false;
            foreach ($dataItems as $dataItem) {
                if ($source->getSourceName() === $dataItem->getName()) {
                    $sourceFound = true;
                    break;
                }
            }
            if (!$sourceFound) {
                $invalidSources[] = $source;
            }
        }

        if (count($invalidSources) > 0) {
            $invalidSources = array_map(function (MappingFromRawConfiguration $source) {
                return sprintf('"%s"', $source->getSourceName());
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
    }

    /**
     * @param FileItem[] $manifests
     * @param MappingFromRawConfiguration[] $configurationSource
     */
    public function validateManifestWithConfiguration(array $manifests, array $configurationSource): void
    {
    }
}
