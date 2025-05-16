<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Table;

use Keboola\OutputMapping\Configuration\Adapter;
use Keboola\OutputMapping\Mapping\MappingFromProcessedConfiguration;
use Keboola\OutputMapping\Mapping\MappingFromRawConfigurationAndPhysicalDataWithManifest;
use Keboola\OutputMapping\MappingCombiner\MappingCombinerInterface;
use Keboola\OutputMapping\SourcesValidator\SourcesValidatorInterface;
use Keboola\OutputMapping\Writer\FileItem;
use Keboola\OutputMapping\Writer\Table\Source\SourceInterface;
use Keboola\StagingProvider\Staging\File\FileStagingInterface;
use Keboola\StagingProvider\Staging\StagingInterface;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\LoggerInterface;

interface StrategyInterface
{
    /**
     * @param Adapter::FORMAT_* $format
     */
    public function __construct(
        ClientWrapper $clientWrapper,
        LoggerInterface $logger,
        StagingInterface $dataStorage,
        FileStagingInterface $metadataStorage,
        string $format,
        bool $isFailedJob,
    );

    public function getDataStorage(): StagingInterface;

    public function getMetadataStorage(): FileStagingInterface;

    public function prepareLoadTaskOptions(MappingFromProcessedConfiguration $source): array;

    public function readFileManifest(FileItem $manifest): array;

    /**
     * @return SourceInterface[]
     */
    public function listSources(string $dir, array $configurations): array;

    /**
     * @return FileItem[] Indexed by file path.
     */
    public function listManifests(string $dir): array;

    public function getSourcesValidator(): SourcesValidatorInterface;

    public function getMappingCombiner(): MappingCombinerInterface;

    public function hasSlicer(): bool;

    /**
     * @param MappingFromRawConfigurationAndPhysicalDataWithManifest[] $combinedMapping
     */
    public function sliceFiles(array $combinedMapping, string $dataType): void;
}
