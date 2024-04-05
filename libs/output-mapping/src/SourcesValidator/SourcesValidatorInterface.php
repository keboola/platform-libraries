<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\SourcesValidator;

interface SourcesValidatorInterface
{
    // TODO nahintovany jako SourceItem
    public function validatePhysicalFilesWithManifest(array $dataItems, array $manifests): void;

    public function validatePhysicalFilesWithConfiguration(array $dataItems, array $configurationSource): void;

    public function validateManifestWithConfiguration(array $manifests, array $configurationSource): void;
}
