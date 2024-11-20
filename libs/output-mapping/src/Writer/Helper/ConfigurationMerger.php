<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Helper;

use Keboola\OutputMapping\Exception\InvalidOutputException;

class ConfigurationMerger
{
    public static function mergeConfigurations(array $configFromManifest, array $configFromMapping): array
    {
        $defaults = ['incremental' => false, 'delete_where_operator' => 'eq', 'delimiter' => ',', 'enclosure' => '"'];
        $config = $configFromManifest;
        foreach ($configFromMapping as $key => $value) {
            if (in_array($key, array_keys($defaults))) {
                if (!isset($config[$key]) || ($value && isset($defaults[$key]) && ($value !== $defaults[$key]))) {
                    $config[$key] = $value;
                }
            } elseif ($key === 'metadata') {
                if (!isset($config[$key])) {
                    $config[$key] = [];
                }
                $config[$key] = self::mergeMetadata($config[$key], $configFromMapping[$key]);
            } elseif ($key === 'column_metadata') {
                if (!isset($config[$key])) {
                    $config[$key] = [];
                }
                foreach ($config[$key] as $column => $item) {
                    if (isset($configFromMapping[$key][$column])) {
                        $config[$key][$column] = self::mergeMetadata($item, $configFromMapping[$key][$column]);
                        unset($configFromMapping[$key][$column]);
                    }
                }
                foreach ($configFromMapping[$key] as $column => $item) {
                    $config[$key][$column] = $item;
                }
            } elseif ($key === 'primary_key') {
                if (!isset($config[$key]) || is_array($value)) {
                    if (!empty($config['schema'])) {
                        $config['schema'] = self::mergePrimaryKeyAndSchema($config['schema'], $value);
                    } else {
                        $config[$key] = $value;
                    }
                }
            } elseif ($key === 'table_metadata') {
                $config[$key] = self::mergeKeyValueArray($config[$key] ?? [], $value);
            } elseif ($key === 'schema') {
                $config[$key] = self::mergeSchema($config[$key] ?? [], $value);
            } else {
                if (!isset($config[$key]) || $value) {
                    $config[$key] = $value;
                }
            }
        }
        return $config;
    }

    private static function mergeMetadata(array $target, array $source): array
    {
        $metadataMap = [];
        foreach ($target as $metadataItem) {
            $metadataMap[$metadataItem['key']] = $metadataItem['value'];
        }
        foreach ($source as $metadataItem) {
            $metadataMap[$metadataItem['key']] = $metadataItem['value'];
        }

        $metadataList = [];
        foreach ($metadataMap as $key => $value) {
            $metadataList[] = ['key' => $key, 'value' => $value];
        }

        return $metadataList;
    }

    private static function mergeSchema(array $manifest, array $mapping): array
    {
        foreach ($mapping as $keyColumn => $column) {
            $manifestColumn = array_filter($manifest, fn($v) => $v['name'] === $column['name']);
            if (!$manifestColumn) {
                $manifest[] = $column;
                continue;
            }
            $manifestColumn = reset($manifestColumn);
            foreach ($column as $key => $value) {
                switch ($key) {
                    case 'data_type':
                    case 'metadata':
                        $manifestColumn[$key] = self::mergeKeyValueArray($manifestColumn[$key] ?? [], $value);
                        break;
                    default:
                        $manifestColumn[$key] = $value;
                }
            }
            $manifest[$keyColumn] = $manifestColumn;
        }

        return $manifest;
    }

    private static function mergeKeyValueArray(array $manifest, array $mapping): array
    {
        foreach ($mapping as $key => $value) {
            $manifest[$key] = $value;
        }
        return $manifest;
    }

    private static function mergePrimaryKeyAndSchema(array $manifestSchema, array $primaryKeys): array
    {
        foreach ($primaryKeys as $primaryKey) {
            $manifestColumn = array_filter($manifestSchema, fn($v) => $v['name'] === $primaryKey);
            if (!$manifestColumn) {
                throw new InvalidOutputException(sprintf(
                    'Primary key "%s" not found in manifest file.',
                    $primaryKey,
                ));
            }
            $manifestSchemaKey = array_key_first($manifestColumn);
            $manifestColumn = reset($manifestColumn);
            $manifestColumn['primary_key'] = true;

            $manifestSchema[$manifestSchemaKey] = $manifestColumn;
        }
        return $manifestSchema;
    }
}
