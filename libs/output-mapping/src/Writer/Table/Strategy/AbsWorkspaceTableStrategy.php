<?php

namespace Keboola\OutputMapping\Writer\Table\Strategy;

use Exception;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Writer\Helper\Path;
use Keboola\OutputMapping\Writer\Table\MappingSource;
use MicrosoftAzure\Storage\Blob\BlobRestProxy;
use MicrosoftAzure\Storage\Blob\Models\ListBlobsOptions;

class AbsWorkspaceTableStrategy extends AbstractWorkspaceTableStrategy
{
    protected function createMapping($sourcePathPrefix, $sourceId, $manifestFile, $mapping)
    {
        $sourcePath = Path::join($sourcePathPrefix, $sourceId);

        if ($this->isDirectory($sourcePath)) {
            $sourcePath = Path::ensureTrailingSlash($sourcePath);
        }

        return new MappingSource($sourcePath, $sourcePath, $manifestFile, $mapping);
    }

    /**
     * @param string $sourcePath
     * @return bool
     */
    private function isDirectory($sourcePath)
    {
        $absCredentials = $this->dataStorage->getCredentials();
        $blobClient = BlobRestProxy::createBlobService($absCredentials['connectionString']);

        try {
            $options = new ListBlobsOptions();
            $options->setPrefix($sourcePath);
            $blobs = $blobClient->listBlobs($absCredentials['container'], $options);

            foreach ($blobs->getBlobs() as $blob) {
                /* there can be multiple blobs with the same prefix (e.g `my`, `my-file`, ...), we're checking
                    if there are blobs where the prefix is a directory. (e.g `my/` or `my-file/`) */
                if (strpos($blob->getName(), $sourcePath.'/') === 0) {
                    return true;
                }
            }
        } catch (Exception $e) {
            throw new InvalidOutputException('Failed to list blobs ' . $e->getMessage(), 0, $e);
        }

        return false;
    }
}
