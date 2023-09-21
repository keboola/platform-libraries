<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\File\Strategy;

use InvalidArgumentException;
use Keboola\OutputMapping\Configuration\File\Manifest\Adapter as FileAdapter;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Exception\OutputOperationException;
use Keboola\OutputMapping\Writer\File\FileItem;
use Keboola\OutputMapping\Writer\File\StrategyInterface;
use Keboola\StorageApi\Options\FileUploadOptions;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Finder\Finder;
use Symfony\Component\Finder\SplFileInfo;
use Throwable;

class Local extends AbstractFileStrategy implements StrategyInterface
{
    public function listManifests(string $dir): array
    {
        try {
            $finder = new Finder();
            $manifests = $finder
                ->files()->name('*.manifest')
                ->in($this->dataStorage->getPath() . '/' . $dir)
                ->depth(0);
        } catch (InvalidArgumentException $e) {
            throw new OutputOperationException(sprintf('Failed to list files: "%s".', $e->getMessage()), $e);
        }
        $manifestFileNames = [];
        $fs = new Filesystem();
        /** @var SplFileInfo $manifest */
        foreach ($manifests as $manifest) {
            $path = $fs->makePathRelative($manifest->getPath(), $this->metadataStorage->getPath());
            $pathName = $path . $manifest->getFilename();
            $manifestFileNames[$pathName] = new FileItem($pathName, $path, $manifest->getFilename());
        }
        return $manifestFileNames;
    }

    public function listFiles(string $dir): array
    {
        try {
            $finder = new Finder();
            /** @var SplFileInfo[] $foundFiles */
            $foundFiles = $finder
                ->files()
                ->notName('*.manifest')
                ->in($this->metadataStorage->getPath() . '/' . $dir)
                ->depth(0);
        } catch (InvalidArgumentException $e) {
            throw new OutputOperationException(sprintf('Failed to list files: "%s".', $e->getMessage()), $e);
        }
        $files = [];
        $fs = new Filesystem();
        foreach ($foundFiles as $file) {
            $path = $fs->makePathRelative($file->getPath(), $this->metadataStorage->getPath());
            $pathName = $path . $file->getFilename();
            $files[$pathName] = new FileItem($pathName, $path, $file->getFilename());
        }
        return $files;
    }

    public function loadFileToStorage(string $file, array $storageConfig): string
    {
        $storageConfig = $this->preProcessStorageConfig($storageConfig);
        $options = new FileUploadOptions();
        $options
            ->setTags(array_unique($storageConfig['tags']))
            ->setIsPermanent($storageConfig['is_permanent'])
            ->setIsEncrypted($storageConfig['is_encrypted'])
            ->setIsPublic($storageConfig['is_public'])
            ->setNotify($storageConfig['notify']);
        return (string) $this->clientWrapper->getTableAndFileStorageClient()
            ->uploadFile($this->dataStorage->getPath() . '/' . $file, $options);
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
}
