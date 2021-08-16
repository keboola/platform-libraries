<?php

namespace Keboola\OutputMapping\Writer\Table\Strategy;

use Exception;
use Keboola\OutputMapping\DeferredTasks\LoadTable;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Writer\Table\Source\SourceInterface;
use MicrosoftAzure\Storage\Blob\BlobRestProxy;
use MicrosoftAzure\Storage\Blob\Models\ListBlobsOptions;

class AbsWorkspaceTableStrategy extends WorkspaceTableStrategy
{
    public function loadDataIntoTable(SourceInterface $source, $tableId, array $options)
    {
        $sourcePath = $this->ensurePathDelimiter($source->getSourcePathPrefix()) . $source->getSourceId();

        if ($this->isSlicedSourcePath($sourcePath)) {
            $sourcePath .= '/';
        }

        return new LoadTable($this->clientWrapper->getBasicClient(), $tableId, [
            'dataWorkspaceId' => $this->dataStorage->getWorkspaceId(),
            'dataObject' => $sourcePath,
            'incremental' => $options['incremental'],
            'columns' => $options['columns'],
        ]);
    }

    /**
     * @param string $sourcePath
     * @return bool
     */
    private function isSlicedSourcePath($sourcePath)
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

    /**
     * @param string $path
     * @return string
     */
    private function ensurePathDelimiter($path)
    {
        return rtrim($path, '/') . '/';
    }
}
