<?php

namespace Keboola\OutputMapping\DeferredTasks\Metadata;

use Keboola\StorageApi\Metadata;
use Keboola\Utils\Sanitizer\ColumnNameSanitizer;

class ColumnMetadata implements MetadataInterface
{
    /** @var string */
    private $tableId;

    /** @var string */
    private $provider;

    /** @var array<string, array> */
    private $metadata;

    /**
     * @param string $tableId
     * @param string $provider
     * @param array<string, array> $metadata
     */
    public function __construct($tableId, $provider, $metadata)
    {
        $this->tableId = $tableId;
        $this->provider = $provider;
        $this->metadata = $metadata;
    }

    public function apply(Metadata $apiClient)
    {
        foreach ($this->metadata as $column => $metadataArray) {
            $columnId = $this->tableId . '.' . ColumnNameSanitizer::sanitize($column);
            $apiClient->postColumnMetadata($columnId, $this->provider, $metadataArray);
        }
    }
}
