<?php

namespace Keboola\OutputMapping\Mapping;

class MappingFromRawConfiguration
{
    private string $delimiter;
    private array $columns;
    private string $destination;
    private string $source;
    private array $mappingItem;

    public function __construct(array $mappingItem)
    {
        $this->mappingItem = $mappingItem;
        // TODO: validate
        $this->source = $mappingItem['source'];
        $this->destination = $mappingItem['destination'];
        $this->columns = $mappingItem['columns'] ?? [];
        $this->delimiter = $mappingItem['delimiter'] ?? ',';
    }

    public function getSourceName(): string
    {
        return $this->source;
    }

    public function asArray(): array
    {
        return $this->mappingItem;
    }
}
