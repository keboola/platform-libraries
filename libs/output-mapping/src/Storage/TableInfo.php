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

    /**
     * Description stored in the native Storage description field, or null when the table has none. Read from
     * the table definition, the same source input mapping reads.
     */
    public function getDescription(): ?string
    {
        $description = $this->tableInfo['definition']['description'] ?? null;

        return is_string($description) && $description !== '' ? $description : null;
    }

    /**
     * Descriptions stored in the native Storage description field of the columns, keyed by column name.
     * Columns without a description are omitted.
     *
     * @return array<string, string>
     */
    public function getColumnDescriptions(): array
    {
        $columns = $this->tableInfo['definition']['columns'] ?? [];
        if (!is_array($columns)) {
            return [];
        }

        $descriptions = [];
        foreach ($columns as $column) {
            if (!is_array($column) || !isset($column['name'])) {
                continue;
            }
            $description = $column['definition']['description'] ?? null;
            if (is_string($description) && $description !== '') {
                $descriptions[(string) $column['name']] = $description;
            }
        }

        return $descriptions;
    }
}
