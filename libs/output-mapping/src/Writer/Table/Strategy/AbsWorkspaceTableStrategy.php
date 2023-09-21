<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Table\Strategy;

use Keboola\FileStorage\Abs\ClientFactory;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Writer\Helper\Path;
use Keboola\OutputMapping\Writer\Table\Source\WorkspaceItemSource;
use MicrosoftAzure\Storage\Blob\Models\ListBlobsOptions;
use Throwable;

class AbsWorkspaceTableStrategy extends AbstractWorkspaceTableStrategy
{
    protected function createSource(string $sourcePathPrefix, string $sourceName): WorkspaceItemSource
    {
        $sourcePath = Path::join($sourcePathPrefix, $sourceName);
        $isSliced = $this->isDirectory($sourcePath);

        if ($isSliced) {
            $sourcePath = Path::ensureTrailingSlash($sourcePath);
        }

        return new WorkspaceItemSource(
            $sourceName,
            $this->dataStorage->getWorkspaceId(),
            $sourcePath,
            $isSliced,
        );
    }

    private function isDirectory(string $sourcePath): bool
    {
        /** @var array{container: string, connectionString: string} $absCredentials */
        $absCredentials = $this->dataStorage->getCredentials();
        $blobClient = ClientFactory::createClientFromConnectionString($absCredentials['connectionString']);

        try {
            $options = new ListBlobsOptions();
            $options->setPrefix($sourcePath);
            $blobs = $blobClient->listBlobs($absCredentials['container'], $options);

            foreach ($blobs->getBlobs() as $blob) {
                /* there can be multiple blobs with the same prefix (e.g `my`, `my-file`, ...), we're checking
                    if there are blobs where the prefix is a directory. (e.g `my/` or `my-file/`) */
                if (str_starts_with($blob->getName(), $sourcePath . '/')) {
                    return true;
                }
            }
        } catch (Throwable $e) {
            throw new InvalidOutputException('Failed to list blobs ' . $e->getMessage(), 0, $e);
        }

        return false;
    }
}
