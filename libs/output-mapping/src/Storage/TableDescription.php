<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Storage;

use Keboola\OutputMapping\Mapping\MappingFromProcessedConfiguration;

/**
 * Table and column descriptions produced by output mapping for a single destination table.
 */
class TableDescription
{
    /**
     * @param array<string, string> $columnDescriptions column name => description
     */
    public function __construct(
        private readonly string $tableId,
        private readonly ?string $tableDescription,
        private readonly array $columnDescriptions,
    ) {
    }

    public static function createFromMapping(MappingFromProcessedConfiguration $source): self
    {
        return new self(
            $source->getDestination()->getTableId(),
            $source->getTableDescription(),
            $source->getColumnDescriptions(),
        );
    }

    public function getTableId(): string
    {
        return $this->tableId;
    }

    public function getTableDescription(): ?string
    {
        return $this->tableDescription;
    }

    /**
     * @return array<string, string> column name => description
     */
    public function getColumnDescriptions(): array
    {
        return $this->columnDescriptions;
    }

    public function isEmpty(): bool
    {
        return $this->tableDescription === null && $this->columnDescriptions === [];
    }
}
