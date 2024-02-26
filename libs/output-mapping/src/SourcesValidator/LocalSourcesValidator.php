<?php

namespace Keboola\OutputMapping\SourcesValidator;

use Exception;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Lister\PhysicalItem;
use Keboola\OutputMapping\Writer\FileItem;

class LocalSourcesValidator implements SourcesValidatorInterface
{

    /**
     * @param FileItem[] $dataItems
     * @param FileItem[] $manifests
     * @return void
     */
    public function validate(array $dataItems, array $manifests): void
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
            // TODO: use proper exception
            throw new Exception('Orphaned manifest files found: ' . implode(', ', $orphanedManifestSourceNames));
        }

        // TODO validated that there is not a $configurationSource that is not in $dataItems
        /*
         *         if (count($mappingsBySource) > 0) {
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

         */
    }
}
