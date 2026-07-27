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

    /**
     * Whether the table description is managed by the system (true) or by the user (false).
     *
     * Output mapping may store/update a system-managed description but must never overwrite a user-managed
     * one (AJDA-2946). Storage defaults the flag to system-managed, which is also assumed when the flag is
     * missing from the table detail (Storage version without DMD-1662).
     */
    public function isDescriptionSystemManaged(): bool
    {
        return (bool) ($this->tableInfo['isDescriptionSystemManaged'] ?? true);
    }

    public function getPrimaryKey(): array
    {
        return $this->tableInfo['primaryKey'];
    }
}
