<?php

namespace Keboola\OutputMapping\Writer\Table\Strategy;

use Exception;
use Keboola\FileStorage\Abs\ClientFactory;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Writer\Helper\Path;
use Keboola\OutputMapping\Writer\Table\Source\WorkspaceItemSource;
use MicrosoftAzure\Storage\Blob\Models\ListBlobsOptions;

class AbsWorkspaceTableStrategy extends AbstractWorkspaceTableStrategy
{
    protected function createSource($sourcePathPrefix, $sourceName)
    {
        $sourcePath = Path::join($sourcePathPrefix, $sourceName);
        $isSliced = $this->isDirectory($sourcePath);

        if ($isSliced) {
            $sourcePath = Path::ensureTrailingSlash($sourcePath);
        }

        return new WorkspaceItemSource(
            $sourceName,
            (string) $this->dataStorage->getWorkspaceId(),
            $sourcePath,
            $isSliced
        );
    }

    /**
     * @param string $sourcePath
     * @return bool
     */
    private function isDirectory($sourcePath)
    {
        $absCredentials = $this->dataStorage->getCredentials();
        $blobClient = ClientFactory::createClientFromConnectionString($absCredentials['connectionString']);

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
