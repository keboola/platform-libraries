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
            $config['columns'] = self::removeRestrictedColumnsFromColumns($config['columns']);
        }

        if (!empty($config['column_metadata'])) {
            $config['column_metadata'] = self::removeRestrictedColumnsFromColumnMetadata($config['column_metadata']);
        }

        return $config;
    }

    public static function removeRestrictedColumnsFromColumns(array $columns): array
    {
        return array_filter($columns, function ($column): bool {
            return self::isRestrictedColumn((string) $column);
        });
    }

    public static function removeRestrictedColumnsFromColumnMetadata(array $columnMetadata): array
    {
        $columnNames = array_keys($columnMetadata);

        $columnNamesFiltered = array_filter($columnNames, function ($column) {
            return self::isRestrictedColumn((string) $column);
        });

        return array_diff_key(
            $columnMetadata,
            array_flip(
                array_diff($columnNames, $columnNamesFiltered),
            ),
        );
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
