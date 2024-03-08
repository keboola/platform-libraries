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

    public function getId(): string
    {
        return $this->tableInfo['id'];
    }

    public function isTyped(): bool
    {
        return $this->tableInfo['isTyped'];
    }

    public function getPrimaryKey()
    {
        return $this->tableInfo['primaryKey'];
    }
}
