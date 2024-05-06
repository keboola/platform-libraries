<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\SourcesValidator;

use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Mapping\MappingFromRawConfiguration;

class WorkspaceSourcesValidator implements SourcesValidatorInterface
{
    public function __construct(private readonly bool $isFailedJob)
    {
    }

    public function validatePhysicalFilesWithManifest(array $dataItems, array $manifests): void
    {
    }

    public function validatePhysicalFilesWithConfiguration(array $dataItems, array $configurationSource): void
    {
    }

    public function validateManifestWithConfiguration(array $manifests, array $configurationSource): void
    {
        if ($this->isFailedJob) {
            // if the job fails, we should not validate the sources, because write_always tables can be processed
            return;
        }
        $invalidManifests = [];
        foreach ($configurationSource as $source) {
            $manifestFound = false;
            foreach ($manifests as $manifest) {
                if ($manifest->getName() === $source->getSourceName() . '.manifest') {
                    $manifestFound = true;
                    break;
                }
            }
            if (!$manifestFound) {
                $invalidManifests[] = $source;
            }
        }

        if (count($invalidManifests) > 0) {
            $invalidManifests = array_map(function (MappingFromRawConfiguration $source) {
                return sprintf('"%s"', $source->getSourceName());
            }, $invalidManifests);

            throw new InvalidOutputException(
                sprintf('Table with manifests not found: %s', implode(', ', $invalidManifests)),
                404,
            );
        }
    }
}
