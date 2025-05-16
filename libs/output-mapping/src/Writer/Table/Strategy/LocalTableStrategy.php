<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Table\Strategy;

use InvalidArgumentException;
use Keboola\OutputMapping\Configuration\Adapter;
use Keboola\OutputMapping\Configuration\Table\Manifest\Adapter as TableAdapter;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Exception\OutputOperationException;
use Keboola\OutputMapping\Mapping\MappingFromProcessedConfiguration;
use Keboola\OutputMapping\Mapping\MappingFromRawConfigurationAndPhysicalDataWithManifest;
use Keboola\OutputMapping\MappingCombiner\LocalMappingCombiner;
use Keboola\OutputMapping\MappingCombiner\MappingCombinerInterface;
use Keboola\OutputMapping\SourcesValidator\LocalSourcesValidator;
use Keboola\OutputMapping\SourcesValidator\SourcesValidatorInterface;
use Keboola\OutputMapping\Writer\FileItem;
use Keboola\OutputMapping\Writer\Helper\Path;
use Keboola\OutputMapping\Writer\Helper\SliceCommandBuilder;
use Keboola\OutputMapping\Writer\Table\StrategyInterface;
use Keboola\StagingProvider\Staging\File\FileStagingInterface;
use Keboola\StagingProvider\Staging\StagingInterface;
use Keboola\StorageApi\Options\FileUploadOptions;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\LoggerInterface;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Finder\Finder;
use Symfony\Component\Finder\SplFileInfo;
use Symfony\Component\Process\Process;
use Throwable;

class LocalTableStrategy implements StrategyInterface
{
    private readonly FileStagingInterface $dataStorage;

    /**
     * @param Adapter::FORMAT_* $format
     */
    public function __construct(
        private readonly ClientWrapper $clientWrapper,
        private readonly LoggerInterface $logger,
        StagingInterface $dataStorage,
        private readonly FileStagingInterface $metadataStorage,
        private readonly string $format,
        private readonly bool $isFailedJob = false,
    ) {
        if (!$dataStorage instanceof FileStagingInterface) {
            throw new InvalidArgumentException('Data storage must be instance of FileStagingInterface');
        }

        $this->dataStorage = $dataStorage;
    }

    public function getDataStorage(): FileStagingInterface
    {
        return $this->dataStorage;
    }

    public function getMetadataStorage(): FileStagingInterface
    {
        return $this->metadataStorage;
    }

    public function prepareLoadTaskOptions(MappingFromProcessedConfiguration $source): array
    {
        $loadOptions = [
            'delimiter' => $source->getDelimiter(),
            'enclosure' => $source->getEnclosure(),
        ];

        if ($source->isSliced()) {
            $loadOptions['dataFileId'] = $this->uploadSlicedFile(
                $source->getPathName(),
                $source->getSourceName(),
                $source->getTags(),
            );
        } else {
            $loadOptions['dataFileId'] = $this->uploadRegularFile($source->getPathName(), $source->getTags());
        }

        return $loadOptions;
    }

    private function uploadSlicedFile(string $pathName, string $baseName, array $tags): string
    {
        $finder = new Finder();
        $slices = $finder->files()->in($pathName)->depth(0);
        $sliceFiles = [];
        foreach ($slices as $slice) {
            $sliceFiles[] = $slice->getPathname();
        }

        $fileUploadOptions = (new FileUploadOptions())
            ->setIsSliced(true)
            ->setFileName($baseName)
            ->setCompress(true)
            ->setTags($tags)
        ;

        return (string) $this->clientWrapper->getTableAndFileStorageClient()->uploadSlicedFile(
            $sliceFiles,
            $fileUploadOptions,
        );
    }

    private function uploadRegularFile(string $pathName, array $tags): string
    {
        $fileUploadOptions = (new FileUploadOptions())
            ->setCompress(true)
            ->setTags($tags)
        ;

        return (string) $this->clientWrapper->getTableAndFileStorageClient()->uploadFile(
            $pathName,
            $fileUploadOptions,
        );
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
            return [];
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

    public function listSources(string $dir, array $configurations): array
    {
        try {
            $dir = Path::join($this->dataStorage->getPath(), $dir);
            $foundFiles = (new Finder())->notName('*.manifest')->in($dir)->depth(0);
        } catch (InvalidArgumentException $e) {
            throw new OutputOperationException(sprintf('Failed to list files: "%s".', $e->getMessage()), $e);
        }
        $files = [];
        $fs = new Filesystem();
        foreach ($foundFiles as $file) {
            $path = $fs->makePathRelative($file->getPath(), $this->metadataStorage->getPath());
            $pathName = $path . $file->getFilename();
            $files[$pathName] = new FileItem($file->getPathname(), $path, $file->getBasename(), $file->isDir());
        }
        return $files;
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

    public function getSourcesValidator(): SourcesValidatorInterface
    {
        return new LocalSourcesValidator($this->isFailedJob);
    }

    public function getMappingCombiner(): MappingCombinerInterface
    {
        return new LocalMappingCombiner();
    }

    public function hasSlicer(): bool
    {
        return true;
    }

    /**
     * @param MappingFromRawConfigurationAndPhysicalDataWithManifest[] $combinedMapping
     */
    public function sliceFiles(array $combinedMapping, string $dataType): void
    {
        foreach ($combinedMapping as $combinedMappingItem) {
            $outputDirPath = uniqid($combinedMappingItem->getPathName() . '-', true);

            $process = SliceCommandBuilder::createProcess(
                sourceName: $combinedMappingItem->getSourceName(),
                inputPath: $combinedMappingItem->getPathName(),
                outputPath: $outputDirPath,
                dataType: $dataType,
            );

            $result = $process->run(function ($type, $buffer) {
                if ($type === Process::OUT) {
                    $this->logger->info(trim($buffer));
                }
            });

            if ($result === SliceCommandBuilder::SLICER_SKIPPED_EXIT_CODE) {
                continue;
            }

            if ($result !== 0) {
                $this->logger->error('Slicer failed', ['process' => $process]);
                continue;
            }

            $filesystem = new Filesystem();
            $filesystem->remove([$combinedMappingItem->getPathName()]);
            $filesystem->rename($outputDirPath, $combinedMappingItem->getPathName());
            $filesystem->rename(
                $outputDirPath . '.manifest',
                $combinedMappingItem->getPathNameManifest(),
                true,
            );
        }
    }
}
