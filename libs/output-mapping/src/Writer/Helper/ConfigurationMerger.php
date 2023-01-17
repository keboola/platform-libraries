<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Helper;

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
}
