<?php

namespace Keboola\OutputMapping\SourcesValidator;

interface SourcesValidatorInterface
{
    // TODO nahintovany jako SourceItem
    public function validatePhysicalFilesWithManifest(array $manifests, array $dataItems): void;

    public function validatePhysicalFilesWithConfiguration(array $dataItems, array $configurationSource, bool $isFailedJob): void;

    public function validateManifestWithConfiguration(array $manifests, array $configurationSource): void;
}
