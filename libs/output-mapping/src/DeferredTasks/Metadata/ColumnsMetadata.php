<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\DeferredTasks\Metadata;

use Keboola\StorageApi\Metadata;
use Keboola\StorageApi\Options\Metadata\TableMetadataUpdateOptions;

class ColumnMetadata implements MetadataInterface
{
    public function __construct(
        private readonly string $tableId,
        private readonly string $provider,
        private readonly array $metadata,
    ) {
    }

    public function apply(Metadata $apiClient, int $bulkSize = 100): void
    {
        assert($bulkSize > 0);
        foreach (array_chunk($this->metadata, $bulkSize, true) as $chunk) {
            $columnsMetadata = [];

            foreach ($chunk as $columnName => $metadataArray) {
                $columnMetadata = [];
                foreach ($metadataArray as $metadata) {
                    $columnMetadata[] = [
                        'columnName' => strval($columnName),
                        'key' => (string) $metadata['key'],
                        'value' => (string) $metadata['value'],
                    ];
                }

                $columnsMetadata[$columnName] = $columnMetadata;
            }

            $options = new TableMetadataUpdateOptions(
                $this->tableId,
                $this->provider,
                null,
                $columnsMetadata,
            );

            $apiClient->postTableMetadataWithColumns($options);
        }
    }
}
