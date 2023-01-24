<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\DeferredTasks\Metadata;

use Keboola\StorageApi\Metadata;
use Keboola\StorageApi\Options\Metadata\TableMetadataUpdateOptions;
use Keboola\Utils\Sanitizer\ColumnNameSanitizer;

class ColumnMetadata implements MetadataInterface
{
    /**
     * @param array<string, array> $metadata
     */
    public function __construct(
        private readonly string $tableId,
        private readonly string $provider,
        private readonly array $metadata
    ) {
    }

    public function apply(Metadata $apiClient): void
    {
        $columnsMetadata = [];
        foreach ($this->metadata as $column => $metadataArray) {
            $columnMetadata = [];
            foreach ($metadataArray as $metadata) {
                $columnMetadata[] = [
                    'key' => (string) $metadata['key'],
                    'value' => (string) $metadata['value'],
                ];
            }

            $columnsMetadata[ColumnNameSanitizer::sanitize($column)] = $columnMetadata;
        }

        $options = new TableMetadataUpdateOptions(
            $this->tableId,
            $this->provider,
            null,
            $columnsMetadata
        );

        $apiClient->postTableMetadataWithColumns($options);
    }
}
