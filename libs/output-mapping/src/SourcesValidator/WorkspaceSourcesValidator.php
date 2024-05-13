<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\SourcesValidator;

use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Mapping\MappingFromRawConfiguration;

class WorkspaceSourcesValidator implements SourcesValidatorInterface
{
    public function validatePhysicalFilesWithManifest(array $dataItems, array $manifests): void
    {
    }

    public function validatePhysicalFilesWithConfiguration(array $dataItems, array $configurationSource): void
    {
    }

    public function validateManifestWithConfiguration(array $manifests, array $configurationSource): void
    {
    }
}
