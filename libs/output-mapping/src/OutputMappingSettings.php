<?php

declare(strict_types=1);

namespace Keboola\OutputMapping;

use Keboola\OutputMapping\Mapping\MappingFromRawConfiguration;
use Keboola\StorageApiBranch\StorageApiToken;

class OutputMappingSettings
{
    private ?string $bucket;
    /** @var MappingFromRawConfiguration[] */
    private array $mapping = [];
    private string $sourcePathPrefix;

    public const OUTPUT_MAPPING_SLICE_FEATURE = 'output-mapping-slice';

    public const TAG_STAGING_FILES_FEATURE = 'tag-staging-files';

    public const NATIVE_TYPES_FEATURE = 'native-types';

    private StorageApiToken $storageApiToken;

    private bool $isFailedJob;

    public function __construct(
        array $configuration,
        string $sourcePathPrefix,
        StorageApiToken $storageApiToken,
        bool $isFailedJob,
    ) {
        // TODO: validate
        $this->bucket = $configuration['bucket'] ?? null;
        foreach ($configuration['mapping'] ?? [] as $mappingItem) {
            $this->mapping[] = new MappingFromRawConfiguration($mappingItem);
        }
        $this->sourcePathPrefix = $sourcePathPrefix;
        $this->storageApiToken = $storageApiToken;
        $this->isFailedJob = $isFailedJob;
    }

    public function hasSlicingFeature(): bool
    {
        return $this->storageApiToken->hasFeature(self::OUTPUT_MAPPING_SLICE_FEATURE);
    }

    public function hasTagStagingFilesFeature(): bool
    {
        return $this->storageApiToken->hasFeature(self::TAG_STAGING_FILES_FEATURE);
    }

    public function hasNativeTypesFeature(): bool
    {
        return $this->storageApiToken->hasFeature(self::NATIVE_TYPES_FEATURE);
    }

    public function getSourcePathPrefix(): string
    {
        return $this->sourcePathPrefix;
    }

    /**
     * @return array<MappingFromRawConfiguration>
     * @deprecated
     */
    public function getMapping(): array
    {
        return $this->mapping;
    }

    public function getDefaultBucket(): ?string
    {
        return $this->bucket;
    }

    public function isFailedJob(): bool
    {
        return $this->isFailedJob;
    }
}
