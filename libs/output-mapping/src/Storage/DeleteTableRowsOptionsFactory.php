<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Storage;

class DeleteTableRowsOptionsFactory
{
    public static function createFromLegacyDeleteWhereColumn(
        string $column,
        string $operator,
        array $values,
    ): array {
        return [
            'whereColumn' => $column,
            'whereOperator' => $operator,
            'whereValues' => $values,
        ];
    }
}
