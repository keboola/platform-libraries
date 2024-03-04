<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Strategy;

use InvalidArgumentException;
use Keboola\OutputMapping\Exception\OutputOperationException;
use Keboola\OutputMapping\Writer\FileItem;
use Keboola\OutputMapping\Writer\Helper\Path;
use Keboola\OutputMapping\Writer\SourceInterface;
use Keboola\OutputMapping\Writer\Table\WorkspaceItemSource;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Finder\Finder;

abstract class AbstractWorkspaceTableStrategy extends AbstractTableStrategy
{
    /**
     * @return array {
     *      dataWorkspaceId: string,
     *      dataObject: string
     * }
     */
    public function prepareLoadTaskOptions(SourceInterface $source, array $config): array
    {
        if (!$source instanceof WorkspaceItemSource) {
            throw new InvalidArgumentException(sprintf(
                'Argument $source is expected to be instance of %s, %s given',
                WorkspaceItemSource::class,
                get_class($source),
            ));
        }

        return [
            'dataWorkspaceId' => $source->getWorkspaceId(),
            'dataObject' => $source->getDataObject(),
        ];
    }

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

    public function hasSlicer(): bool
    {
        return false;
    }

    public function sliceFiles(): void
    {
        throw new \Exception('Not implemented');
    }
}
