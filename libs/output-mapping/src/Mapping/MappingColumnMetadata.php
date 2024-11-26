<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Mapping;

class MappingColumnMetadata
{
    public function __construct(
        private readonly string $columnName,
        private readonly array $metadata,
    ) {
    }

    public function getColumnName(): string
    {
        return $this->columnName;
    }

    public function getMetadata(): array
    {
        return $this->metadata;
    }
}
