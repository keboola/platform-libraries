<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Mapping;

use Keboola\OutputMapping\Configuration\Table\Manifest;

class MappingFromRawConfiguration
{
    private string $delimiter;
    private string $enclosure;
    private array $columns;
    private string $destination;
    private string $source;
    private array $mappingItem;

    public function __construct(array $mappingItem)
    {
        $this->mappingItem = $mappingItem;
        $this->source = $mappingItem['source'];
        $this->destination = $mappingItem['destination'];
        $this->columns = $mappingItem['columns'] ?? [];
        $this->delimiter = $mappingItem['delimiter'] ?? Manifest::DEFAULT_DELIMITER;
        $this->enclosure = $mappingItem['enclosure'] ?? Manifest::DEFAULT_ENCLOSURE;
    }

    public function getSourceName(): string
    {
        return $this->source;
    }

    public function asArray(): array
    {
        return $this->mappingItem;
    }

    public function getDelimiter(): string
    {
        return $this->delimiter;
    }

    public function getEnclosure(): string
    {
        return $this->enclosure;
    }

    public function getColumns(): array
    {
        return $this->columns;
    }
}
