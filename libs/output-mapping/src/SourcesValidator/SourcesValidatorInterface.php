<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\SourcesValidator;

use Keboola\OutputMapping\Mapping\MappingFromRawConfiguration;
use Keboola\OutputMapping\Writer\Table\Source\SourceInterface;

interface SourcesValidatorInterface
{
    /**
     * @param SourceInterface[] $dataItems
     * @param SourceInterface[] $manifests
     */
    public function validatePhysicalFilesWithManifest(array $dataItems, array $manifests): void;

    /**
     * @param SourceInterface[] $dataItems
     * @param MappingFromRawConfiguration[] $configurationSource
     */
    public function validatePhysicalFilesWithConfiguration(array $dataItems, array $configurationSource): void;

    /**
     * @param SourceInterface[] $manifests
     * @param MappingFromRawConfiguration[] $configurationSource
     */
    public function validateManifestWithConfiguration(array $manifests, array $configurationSource): void;
}
