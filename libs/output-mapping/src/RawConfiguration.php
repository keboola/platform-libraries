<?php

namespace Keboola\OutputMapping;

use Keboola\OutputMapping\Mapping\MappingFromRawConfiguration;

class RawConfiguration
{
    private ?string $bucket;
    /** @var MappingFromRawConfiguration[] */
    private array $mapping;
    private string $sourcePathPrefix;

    public const OUTPUT_MAPPING_SLICE_FEATURE = 'output-mapping-slice';
    private array $features;

    /**
     * @param array $configuration
     */
    public function __construct(array $configuration, string $sourcePathPrefix, array $features)
    {
        // TODO: validate
        $this->bucket = $configuration['bucket'] ?? null;
        foreach ($configuration['mapping'] as $mappingItem) {
            $this->mapping[] = new MappingFromRawConfiguration($mappingItem);
        }
        $this->sourcePathPrefix = $sourcePathPrefix;
        $this->features = $features;
    }

    public function hasSlicingFeature(): bool
    {
        return in_array(self::OUTPUT_MAPPING_SLICE_FEATURE, $this->features, true);
    }

    public function getSourcePathPrefix()
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
}
