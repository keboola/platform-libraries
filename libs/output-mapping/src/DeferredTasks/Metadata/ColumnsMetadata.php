<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\DeferredTasks\Metadata;

use Keboola\OutputMapping\Mapping\MappingColumnMetadata;
use Keboola\StorageApi\Metadata;
use Keboola\StorageApi\Options\Metadata\TableMetadataUpdateOptions;

class ColumnsMetadata implements MetadataInterface
{
    public function __construct(
        private readonly string $tableId,
        private readonly string $provider,
        /** @var MappingColumnMetadata[] $metadata */
        private readonly array $metadata,
    ) {
    }

    public function apply(Metadata $apiClient, int $bulkSize = 100): void
    {
        assert($bulkSize > 0);
        foreach (array_chunk($this->metadata, $bulkSize) as $chunk) {
            $columnsMetadata = [];

            /** @var MappingColumnMetadata[] $chunk */
            foreach ($chunk as $mappingColumnMetadata) {
                $columnMetadata = [];
                foreach ($mappingColumnMetadata->getMetadata() as $metadata) {
                    $columnMetadata[] = [
                        'columnName' => $mappingColumnMetadata->getColumnName(),
                        'key' => (string) $metadata['key'],
                        'value' => (string) $metadata['value'],
                    ];
                }

                // a column may end up with no metadata at all, e.g. when its only entry was the description,
                // which is stored in the native Storage description field instead
                if ($columnMetadata) {
                    $columnsMetadata[$mappingColumnMetadata->getColumnName()] = $columnMetadata;
                }
            }

            if (!$columnsMetadata) {
                continue;
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
