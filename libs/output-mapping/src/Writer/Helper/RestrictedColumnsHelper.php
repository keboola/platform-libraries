<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Helper;

class RestrictedColumnsHelper
{
    private const TIMESTAMP_COLUMN_NAME = '_timestamp';

    public static function removeRestrictedColumnsFromConfig(array $config): array
    {
        if (!empty($config['columns'])) {
            $config['columns'] = array_filter($config['columns'], function ($column): bool {
                return mb_strtolower((string) $column) !== self::TIMESTAMP_COLUMN_NAME;
            });
        }

        if (!empty($config['column_metadata'])) {
            $columnNames = array_keys($config['column_metadata']);

            $columnNamesFiltered = array_filter($columnNames, function ($column) {
                return mb_strtolower((string) $column) !== self::TIMESTAMP_COLUMN_NAME;
            });

            $config['column_metadata'] = array_diff_key(
                $config['column_metadata'],
                array_flip(
                    array_diff($columnNames, $columnNamesFiltered)
                )
            );
        }

        return $config;
    }
}
