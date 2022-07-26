<?php

namespace Keboola\OutputMapping\DeferredTasks\Metadata;

use Keboola\StorageApi\Metadata;
use Keboola\StorageApi\Options\Metadata\TableMetadataUpdateOptions;

class TableMetadata implements MetadataInterface
{
    /** @var string */
    private $tableId;

    /** @var string */
    private $provider;

    /** @var array */
    private $metadata;

    /**
     * @param string $tableId
     * @param string $provider
     * @param array $metadata
     */
    public function __construct($tableId, $provider, $metadata)
    {
        $this->tableId = $tableId;
        $this->provider = $provider;
        $this->metadata = $metadata;
    }

    public function apply(Metadata $apiClient)
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
            $tableMetadata
        ));
    }
}
