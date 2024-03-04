<?php

namespace Keboola\OutputMapping\SourcesValidator;

interface SourcesValidatorInterface
{
    public function validatePhysicalFilesWithManifest(array $manifests, array $dataItems): void;

    public function validatePhysicalFilesWithConfiguration(array $dataItems, array $configurationSource, bool $isFailedJob): void;
}
