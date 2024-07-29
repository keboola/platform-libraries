<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\DeferredTasks\Metadata;

use Keboola\OutputMapping\Mapping\MappingFromConfigurationSchemaColumn;
use Keboola\StorageApi\Metadata;
use Keboola\StorageApi\Options\Metadata\TableMetadataUpdateOptions;

class SchemaColumnMetadata implements MetadataInterface
{
    /** @param MappingFromConfigurationSchemaColumn[] $schema */
    public function __construct(
        private readonly string $tableId,
        private readonly string $provider,
        private readonly array $schema,
    ) {
    }

    public function apply(Metadata $apiClient, int $bulkSize = 100): void
    {
        assert($bulkSize > 0);

        /** @var MappingFromConfigurationSchemaColumn[] $chunk */
        foreach (array_chunk($this->schema, $bulkSize, true) as $chunk) {
            $columnsMetadata = [];

            foreach ($chunk as $metadataArray) {
                $columnMetadata = [];
                $columnName = $metadataArray->getName();
                foreach ($metadataArray->getMetadata() as $key => $value) {
                    $columnMetadata[] = [
                        'columnName' => $columnName,
                        'key' => (string) $key,
                        'value' => (string) $value,
                    ];
                }

                if ($columnMetadata) {
                    $columnsMetadata[$columnName] = $columnMetadata;
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
