<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Helper;

use Keboola\OutputMapping\Exception\InvalidOutputException;

class RestrictedColumnsHelper
{
    private const TIMESTAMP_COLUMN_NAME = '_timestamp';

    public static function removeRestrictedColumnsFromConfig(array $config): array
    {
        if (!empty($config['columns'])) {
            $config['columns'] = array_filter($config['columns'], function ($column): bool {
                return self::isRestrictedColumn((string) $column);
            });
        }

        if (!empty($config['column_metadata'])) {
            $columnNames = array_keys($config['column_metadata']);

            $columnNamesFiltered = array_filter($columnNames, function ($column) {
                return self::isRestrictedColumn((string) $column);
            });

            $config['column_metadata'] = array_diff_key(
                $config['column_metadata'],
                array_flip(
                    array_diff($columnNames, $columnNamesFiltered),
                ),
            );
        }

        return $config;
    }

    public static function validateRestrictedColumnsInConfig(array $columns, array $columnMetadata): void
    {
        $errors = [];
        if (!empty($columns)) {
            $restrictedColumns = array_filter($columns, function ($column): bool {
                return !self::isRestrictedColumn((string) $column);
            });
            if ($restrictedColumns) {
                $errors[] = sprintf(
                    'System columns "%s" cannot be imported to the table.',
                    implode(', ', $restrictedColumns),
                );
            }
        }

        if (!empty($columnMetadata)) {
            $columnNames = array_keys($columnMetadata);
            $restrictedColumns = array_filter($columnNames, function ($column): bool {
                return !self::isRestrictedColumn((string) $column);
            });
            if ($restrictedColumns) {
                $errors[] = sprintf(
                    'Metadata for system columns "%s" cannot be imported to the table.',
                    implode(', ', $restrictedColumns),
                );
            }
        }

        if ($errors) {
            throw new InvalidOutputException(implode(' ', $errors));
        }
    }

    private static function isRestrictedColumn(string $columnName): bool
    {
        return mb_strtolower($columnName) !== self::TIMESTAMP_COLUMN_NAME;
    }
}
