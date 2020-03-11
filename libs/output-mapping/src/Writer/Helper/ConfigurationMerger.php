<?php

namespace Keboola\OutputMapping\Writer\Helper;

class ConfigurationMerger
{
    public static function mergeConfigurations(array $configFromManifest, array $configFromMapping)
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

    private static function mergeMetadata($target, $source)
    {
        // overwrite existing keys
        for ($i = 0; $i < count($target); $i++) {
            for ($j = 0; $j < count($source); $j++) {
                if ($target[$i]['key'] === $source[$j]['key']) {
                    $target[$i]['value'] = $source[$j]['value'];
                    unset($source[$j]);
                }
            }
        }
        // add remaining entries
        foreach ($source as $item) {
            $target[] = $item;
        }
        return $target;
    }

}
