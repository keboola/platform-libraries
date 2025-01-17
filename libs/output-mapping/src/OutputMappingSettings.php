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

    public const OUTPUT_MAPPING_CONNECTION_WEBALIZE = 'output-mapping-connection-webalize';

    public const TAG_STAGING_FILES_FEATURE = 'tag-staging-files';

    public const NATIVE_TYPES_FEATURE = 'native-types';

    public const NEW_NATIVE_TYPES_FEATURE = 'new-native-types';

    public const DATA_TYPES_SUPPORT_AUTHORITATIVE = 'authoritative';

    public const DATA_TYPES_SUPPORT_HINTS = 'hints';

    public const DATA_TYPES_SUPPORT_NONE = 'none';

    public const BIG_QUERY_NATIVE_TYPES_FEATURE = 'bigquery-native-types';

    private StorageApiToken $storageApiToken;

    private bool $isFailedJob;

    private string $dataTypeSupport;

    private ?array $treatValuesAsNull;

    public function __construct(
        array $configuration,
        string $sourcePathPrefix,
        StorageApiToken $storageApiToken,
        bool $isFailedJob,
        string $dataTypeSupport,
    ) {
        // TODO: validate
        $this->bucket = $configuration['bucket'] ?? null;
        $this->treatValuesAsNull = $configuration['treat_values_as_null'] ?? null;
        foreach ($configuration['mapping'] ?? [] as $mappingItem) {
            $this->mapping[] = new MappingFromRawConfiguration($mappingItem);
        }
        $this->sourcePathPrefix = $sourcePathPrefix;
        $this->storageApiToken = $storageApiToken;
        $this->isFailedJob = $isFailedJob;
        $this->dataTypeSupport = $dataTypeSupport;
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
        if ($this->hasNewNativeTypesFeature()) {
            return false;
        }
        return $this->storageApiToken->hasFeature(self::NATIVE_TYPES_FEATURE);
    }

    public function hasNewNativeTypesFeature(): bool
    {
        return $this->storageApiToken->hasFeature(self::NEW_NATIVE_TYPES_FEATURE);
    }

    public function hasConnectionWebalizeFeature(): bool
    {
        return $this->storageApiToken->hasFeature(self::OUTPUT_MAPPING_CONNECTION_WEBALIZE);
    }

    public function getDataTypeSupport(): string
    {
        return $this->dataTypeSupport;
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

    public function hasBigqueryNativeTypesFeature(): bool
    {
        return $this->storageApiToken->hasFeature(self::BIG_QUERY_NATIVE_TYPES_FEATURE);
    }

    public function getTreatValuesAsNull(): ?array
    {
        return $this->treatValuesAsNull;
    }
}
