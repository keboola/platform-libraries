<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\DeferredTasks\Metadata;

use Keboola\StorageApi\Metadata;
use Keboola\StorageApi\Options\Metadata\TableMetadataUpdateOptions;

class TableMetadata implements MetadataInterface
{
    public function __construct(
        private readonly string $tableId,
        private readonly string $provider,
        private readonly array $metadata,
    ) {
    }

    public function apply(Metadata $apiClient): void
    {
        $tableMetadata = [];
        foreach ($this->metadata as $metadata) {
            $tableMetadata[] = [
                'key' => (string) $metadata['key'],
                'value' => (string) $metadata['value'],
            ];
        }

        $apiClient->postTableMetadataWithColumns(new TableMetadataUpdateOptions(
            $this->tableId,
            $this->provider,
            $tableMetadata,
        ));
    }
}
