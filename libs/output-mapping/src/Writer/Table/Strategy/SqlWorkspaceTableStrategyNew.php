<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Table\Strategy;

use Exception;
use InvalidArgumentException;
use Keboola\OutputMapping\Configuration\Table\Manifest\Adapter as TableAdapter;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Exception\OutputOperationException;
use Keboola\OutputMapping\SourcesValidator\SourcesValidatorInterface;
use Keboola\OutputMapping\SourcesValidator\WorkspaceSourcesValidator;
use Keboola\OutputMapping\Writer\FileItem;
use Keboola\OutputMapping\Writer\Helper\Path;
use Keboola\OutputMapping\Writer\Table\Source\WorkspaceItemSource;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Finder\Finder;
use Symfony\Component\Finder\SplFileInfo;
use Throwable;

class SqlWorkspaceTableStrategyNew extends AbstractWorkspaceTableStrategyNew
{
    public function listManifests(string $dir): array
    {
        try {
            $dir = Path::join($this->metadataStorage->getPath(), $dir);
            $foundFiles = (new Finder())->files()->name('*.manifest')->in($dir)->depth(0);
        } catch (InvalidArgumentException $e) {
            throw new OutputOperationException(sprintf('Failed to list files: "%s".', $e->getMessage()), $e);
        }
        $files = [];
        $fs = new Filesystem();
        foreach ($foundFiles as $file) {
            $path = $fs->makePathRelative($file->getPath(), $this->metadataStorage->getPath());
            $pathName = $path . $file->getFilename();
            $files[$pathName] = new FileItem($pathName, $path, $file->getBasename(), false);
        }
        return $files;
    }

    public function listSources(string $dir, array $configurations): array
    {
        $sources = [];
        foreach ($configurations['mapping'] ?? [] as $mapping) {
            $source = $mapping['source'];
            $sources[$source] = new WorkspaceItemSource($source, $this->dataStorage->getWorkspaceId(), $source, false);
        }
        return $sources;
    }

    public function readFileManifest(FileItem $manifest): array
    {
        $manifestFile = sprintf(
            '%s/%s/%s',
            $this->metadataStorage->getPath(),
            $manifest->getPath(),
            $manifest->getName(),
        );
        $adapter = new TableAdapter($this->format);
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

    public function getSourcesValidator(): SourcesValidatorInterface
    {
        return new WorkspaceSourcesValidator();
    }

    public function hasSlicer(): bool
    {
        return false;
    }

    public function sliceFiles(array $combinedMapping): void
    {
        throw new Exception('Not implemented');
    }
}
