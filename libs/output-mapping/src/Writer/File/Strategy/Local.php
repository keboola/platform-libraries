<?php

namespace Keboola\OutputMapping\Writer\File\Strategy;

use Exception;
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

class Local extends AbstractFileStrategy implements StrategyInterface
{
    /**
     * @inheritDoc
     */
    public function listManifests($dir)
    {
        try {
            $finder = new Finder();
            $manifests = $finder->files()->name('*.manifest')->in($dir)->depth(0);
        } catch (InvalidArgumentException $e) {
            throw new OutputOperationException(sprintf('Failed to list files: "%s".', $e->getMessage()), $e);
        }
        $manifestFileNames = [];
        /** @var SplFileInfo $manifest */
        foreach ($manifests as $manifest) {
            $manifestFileNames[$manifest->getPathname()] = new FileItem($manifest->getPathname(), $manifest->getPath(), $manifest->getFilename());
        }
        return $manifestFileNames;
    }

    /**
     * @inheritDoc
     */
    public function listFiles($source)
    {
        try {
            $finder = new Finder();
            /** @var SplFileInfo[] $foundFiles */
            $foundFiles = $finder->files()->notName('*.manifest')->in($source)->depth(0);
        } catch (InvalidArgumentException $e) {
            throw new OutputOperationException(sprintf('Failed to list files: "%s".', $e->getMessage()), $e);
        }
        $files = [];
        foreach ($foundFiles as $file) {
            $files[$file->getPathname()] = new FileItem($file->getPathname(), $file->getPath(), $file->getFilename());
        }
        return $files;
    }

    /**
     * @inheritDoc
     */
    public function loadFileToStorage($source, array $storageConfig)
    {
        $storageConfig = $this->preProcessStorageConfig($storageConfig);
        $options = new FileUploadOptions();
        $options
            ->setTags(array_unique($storageConfig['tags']))
            ->setIsPermanent($storageConfig['is_permanent'])
            ->setIsEncrypted($storageConfig['is_encrypted'])
            ->setIsPublic($storageConfig['is_public'])
            ->setNotify($storageConfig['notify']);
        return $this->clientWrapper->getBasicClient()->uploadFile($source, $options);
    }

    /**
     * @inheritDoc
     */
    public function readFileManifest($manifestFile)
    {
        $adapter = new FileAdapter($this->format);
        $fs = new Filesystem();
        if (!$fs->exists($manifestFile)) {
            throw new InvalidOutputException("File '$manifestFile' not found.");
        }
        try {
            $fileHandler = new SplFileInfo($manifestFile, "", basename($manifestFile));
            $serialized = $fileHandler->getContents();
            return $adapter->deserialize($serialized);
        } catch (Exception $e) {
            throw new InvalidOutputException(
                sprintf('Failed to parse manifest file "%s" as "%s": %s', $manifestFile, $this->format, $e->getMessage()),
                $e->getCode(),
                $e
            );
        }
    }
}
