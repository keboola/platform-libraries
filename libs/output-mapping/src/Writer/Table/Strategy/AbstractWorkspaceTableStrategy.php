<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Table\Strategy;

use Exception;
use InvalidArgumentException;
use Keboola\OutputMapping\Configuration\Adapter;
use Keboola\OutputMapping\Configuration\Table\Manifest\Adapter as TableAdapter;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Exception\OutputOperationException;
use Keboola\OutputMapping\Mapping\MappingFromProcessedConfiguration;
use Keboola\OutputMapping\MappingCombiner\MappingCombinerInterface;
use Keboola\OutputMapping\MappingCombiner\WorkspaceMappingCombiner;
use Keboola\OutputMapping\SourcesValidator\SourcesValidatorInterface;
use Keboola\OutputMapping\SourcesValidator\WorkspaceSourcesValidator;
use Keboola\OutputMapping\Writer\FileItem;
use Keboola\OutputMapping\Writer\Helper\Path;
use Keboola\OutputMapping\Writer\Table\Source\SourceType;
use Keboola\OutputMapping\Writer\Table\StrategyInterface;
use Keboola\StagingProvider\Staging\File\FileStagingInterface;
use Keboola\StagingProvider\Staging\StagingInterface;
use Keboola\StagingProvider\Staging\Workspace\WorkspaceStagingInterface;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\LoggerInterface;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Finder\Finder;
use Symfony\Component\Finder\SplFileInfo;
use Throwable;

abstract class AbstractWorkspaceTableStrategy implements StrategyInterface
{
    protected readonly WorkspaceStagingInterface $dataStorage;

    /**
     * @param Adapter::FORMAT_* $format
     */
    public function __construct(
        protected readonly ClientWrapper $clientWrapper,
        protected readonly LoggerInterface $logger,
        StagingInterface $dataStorage,
        protected readonly FileStagingInterface $metadataStorage,
        protected readonly string $format,
        protected readonly bool $isFailedJob = false,
    ) {
        if (!$dataStorage instanceof WorkspaceStagingInterface) {
            throw new InvalidArgumentException('Data storage must be instance of WorkspaceStagingInterface');
        }

        $this->dataStorage = $dataStorage;
    }

    public function getDataStorage(): WorkspaceStagingInterface
    {
        return $this->dataStorage;
    }

    public function getMetadataStorage(): FileStagingInterface
    {
        return $this->metadataStorage;
    }

    /**
     * @return array {
     *      dataWorkspaceId: string,
     *      dataObject: string
     * }
     */
    public function prepareLoadTaskOptions(MappingFromProcessedConfiguration $source): array
    {
        if ($source->getItemSourceType() !== SourceType::WORKSPACE) {
            throw new InvalidArgumentException(sprintf(
                'Argument $source is expected to be type of "%s", "%s" given',
                SourceType::WORKSPACE->value,
                $source->getItemSourceType()->value,
            ));
        }

        return [
            'dataWorkspaceId' => $source->getWorkspaceId(),
            'dataObject' => $source->getDataObject(),
        ];
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

    public function sliceFiles(array $combinedMapping, string $dataType): void
    {
        throw new Exception('Not implemented');
    }

    public function getSourcesValidator(): SourcesValidatorInterface
    {
        return new WorkspaceSourcesValidator();
    }

    public function getMappingCombiner(): MappingCombinerInterface
    {
        return new WorkspaceMappingCombiner($this->dataStorage->getWorkspaceId());
    }
}
