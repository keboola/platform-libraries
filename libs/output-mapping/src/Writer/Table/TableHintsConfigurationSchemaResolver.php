<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Table;

use Keboola\OutputMapping\Mapping\MappingFromConfigurationSchemaColumnDataType;

class TableHintsConfigurationSchemaResolver
{
    public function resolveColumnsConfiguration(array $processedConfig): array
    {
        if (!isset($processedConfig['schema'])) {
            return $processedConfig;
        }

        $convertedSchemaConfig = [];
        foreach ($processedConfig['schema'] as $item) {
            if (isset($item['data_type'])) {
                $dataTypes = new MappingFromConfigurationSchemaColumnDataType($item['data_type']);
                $item['metadata']['KBC.datatype.basetype'] = $dataTypes->getBaseTypeName();
                if ($dataTypes->getBaseLength() !== null) {
                    $item['metadata']['KBC.datatype.length'] = $dataTypes->getBaseLength();
                }
                if ($dataTypes->getBaseDefaultValue() !== null) {
                    $item['metadata']['KBC.datatype.default'] = $dataTypes->getBaseDefaultValue();
                }
                unset($item['data_type']);
            }
            if (array_key_exists('nullable', $item)) {
                $item['metadata']['KBC.datatype.nullable'] = (int) $item['nullable'];
                unset($item['nullable']);
            }
            if (array_key_exists('distribution_key', $item)) {
                $item['metadata']['KBC.datatype.distribution_key'] = $item['distribution_key'];
                unset($item['distribution_key']);
            }
            $convertedSchemaConfig[] = $item;
        }

        $processedConfig['schema'] = $convertedSchemaConfig;
        return $processedConfig;
    }
}
