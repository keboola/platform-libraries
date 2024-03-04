<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Strategy;

use Keboola\FileStorage\Abs\ClientFactory;
use Keboola\OutputMapping\Configuration\File\Manifest\Adapter as FileAdapter;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Writer\Helper\Path;
use Keboola\OutputMapping\Writer\Table\Source\AbsWorkspaceItemSourceFactory;
use Keboola\OutputMapping\Writer\Table\WorkspaceItemSource;
use MicrosoftAzure\Storage\Blob\Models\ListBlobsOptions;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Finder\SplFileInfo;
use Throwable;

class AbsWorkspaceTableStrategy extends AbstractWorkspaceTableStrategy
{
    public function listSources(string $dir, array $configurations): array
    {
        $sources = [];
        foreach ($configurations['mapping'] ?? [] as $mapping) {
            $source = $mapping['source'];
            $sourcePath = Path::join($dir, $source);
            $isSliced = $this->isDirectory($sourcePath);

            if ($isSliced) {
                $sourcePath = Path::ensureTrailingSlash($sourcePath);
            }

            $sources[$source] = new WorkspaceItemSource($source, $this->dataStorage->getWorkspaceId(), $sourcePath, false);
        }
        return $sources;
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

    public function readFileManifest(string $manifestFile): array
    {
        $manifestFile = $this->metadataStorage->getPath() . '/' . $manifestFile;
        $adapter = new FileAdapter($this->format);
        $fs = new Filesystem();
        if (!$fs->exists($manifestFile)) {
            throw new InvalidOutputException("File '$manifestFile' not found.");
        }
        try {
            $fileHandler = new SplFileInfo($manifestFile, '', basename($manifestFile));
            $serialized = $fileHandler->getContents();
            return $adapter->deserialize($serialized);
        } catch (Throwable $e) {
            throw new InvalidOutputException(
                sprintf(
                    'Failed to parse manifest file "%s" as "%s": %s',
                    $manifestFile,
                    $this->format,
                    $e->getMessage(),
                ),
                $e->getCode(),
                $e,
            );
        }
    }

    public function hasSlicer(): bool
    {
        return false;
    }

    public function sliceFiles(): void
    {
        throw new \Exception('Not implemented');
    }
}
