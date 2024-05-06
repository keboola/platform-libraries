<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Table;

use Keboola\InputMapping\Staging\ProviderInterface;
use Keboola\OutputMapping\Mapping\MappingFromProcessedConfiguration;
use Keboola\OutputMapping\Mapping\MappingFromRawConfigurationAndPhysicalDataWithManifest;
use Keboola\OutputMapping\MappingCombiner\MappingCombinerInterface;
use Keboola\OutputMapping\SourcesValidator\SourcesValidatorInterface;
use Keboola\OutputMapping\Writer\FileItem;
use Keboola\OutputMapping\Writer\Table\Source\SourceInterface;

interface StrategyInterfaceNew
{
    public function getDataStorage(): ProviderInterface;

    public function getMetadataStorage(): ProviderInterface;

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
    public function sliceFiles(array $combinedMapping): void;
}
