<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Storage;

class TableInfo
{
    public function __construct(private readonly array $tableInfo)
    {
    }

    public function getColumns(): array
    {
        return $this->tableInfo['columns'] ?? [];
    }

    /**
     * Table-level metadata as returned by Storage (list of {key, value, provider, ...}).
     */
    public function getMetadata(): array
    {
        return $this->tableInfo['metadata'] ?? [];
    }

    /**
     * Column-level metadata keyed by column name, each a list of {key, value, provider, ...}.
     */
    public function getColumnMetadata(): array
    {
        return $this->tableInfo['columnMetadata'] ?? [];
    }

    public function getId(): string
    {
        return $this->tableInfo['id'];
    }

    public function isTyped(): bool
    {
        return $this->tableInfo['isTyped'];
    }

    public function getPrimaryKey(): array
    {
        return $this->tableInfo['primaryKey'];
    }
}
