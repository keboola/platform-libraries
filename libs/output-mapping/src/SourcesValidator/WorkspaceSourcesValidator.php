<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\SourcesValidator;

use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Lister\PhysicalItem;
use Keboola\OutputMapping\Mapping\MappingFromRawConfiguration;
use Keboola\OutputMapping\Writer\FileItem;

class WorkspaceSourcesValidator implements SourcesValidatorInterface
{

    /**
     * @param FileItem[] $dataItems
     * @param FileItem[] $manifests
     * @return void
     */
    public function validatePhysicalFilesWithManifest(array $dataItems, array $manifests): void
    {
    }

    /**
     * @param FileItem[] $dataItems
     * @param MappingFromRawConfiguration[] $configurationSource
     */
    public function validatePhysicalFilesWithConfiguration(array $dataItems, array $configurationSource): void
    {
    }

    /**
     * @param FileItem[] $manifests
     * @param MappingFromRawConfiguration[] $configurationSource
     */
    public function validateManifestWithConfiguration(array $manifests, array $configurationSource): void
    {
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
