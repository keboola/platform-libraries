<?php

namespace Keboola\OutputMapping\DeferredTasks\Metadata;

use Keboola\StorageApi\Metadata;

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
        $apiClient->postTableMetadata($this->tableId, $this->provider, $this->metadata);
    }
}
