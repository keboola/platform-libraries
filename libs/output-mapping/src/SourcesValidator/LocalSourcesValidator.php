<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\SourcesValidator;

use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Mapping\MappingFromRawConfiguration;

class LocalSourcesValidator implements SourcesValidatorInterface
{
    public function __construct(private readonly bool $isFailedJob)
    {
    }

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
                implode(', ', array_map(fn($v) => sprintf('"%s"', $v), $orphanedManifestSourceNames)),
            ));
        }
    }

    public function validatePhysicalFilesWithConfiguration(array $dataItems, array $configurationSource): void
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
            if (!$this->isFailedJob) {
                throw new InvalidOutputException(
                    sprintf('Table sources not found: %s', implode(', ', $invalidSources)),
                    404,
                );
            }
        }
    }

    public function validateManifestWithConfiguration(array $manifests, array $configurationSource): void
    {
    }
}
