<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Table;

use InvalidArgumentException;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Mapping\MappingFromRawConfigurationAndPhysicalDataWithManifest;
use Keboola\OutputMapping\OutputMappingSettings;
use Keboola\OutputMapping\Writer\Helper\RestrictedColumnsHelper;
use Keboola\OutputMapping\Writer\Table\Strategy\SqlWorkspaceTableStrategy;

class TableConfigurationValidator
{
    public function __construct(
        readonly private StrategyInterface $strategy,
        readonly private OutputMappingSettings $settings,
    ) {
    }

    public function validate(
        MappingFromRawConfigurationAndPhysicalDataWithManifest $source,
        array $config,
    ): void {
        if (!$this->strategy instanceof SqlWorkspaceTableStrategy) {
            $this->validateRestrictedColumns($source, $config);
        }

        if ($this->settings->hasNewNativeTypesFeature()) {
            $this->validateSchemaConfig($config);
        }

        $this->validateColumnsSpecification($source, $config);

        $this->validateMappingDestination($config);
    }

    private function validateRestrictedColumns(
        MappingFromRawConfigurationAndPhysicalDataWithManifest $source,
        array $config,
    ): void {
        try {
            RestrictedColumnsHelper::validateRestrictedColumnsInConfig(
                $config['columns'],
                $config['column_metadata'],
                $config['schema'],
            );
        } catch (InvalidOutputException $e) {
            throw new InvalidOutputException(
                sprintf(
                    'Failed to process mapping for table %s: %s',
                    $source->getSourceName(),
                    $e->getMessage(),
                ),
                0,
                $e,
            );
        }
    }

    private function validateSchemaConfig(array $config): void
    {
        if ($this->settings->getDataTypeSupport() === OutputMappingSettings::DATA_TYPES_SUPPORT_NONE) {
            return;
        }

        if (empty($config['schema'])) {
            throw new InvalidOutputException('Configuration schema is missing.');
        }

        if ($this->settings->getDataTypeSupport() === OutputMappingSettings::DATA_TYPES_SUPPORT_AUTHORITATIVE) {
            $missingDataType = array_filter($config['schema'], fn($column) => !isset($column['data_type']['base']));

            if ($missingDataType) {
                throw new InvalidOutputException(
                    sprintf(
                        'Missing data type for columns: %s',
                        implode(', ', array_map(fn($column) => $column['name'], $missingDataType)),
                    ),
                );
            }
        }
    }

    private function validateColumnsSpecification(
        MappingFromRawConfigurationAndPhysicalDataWithManifest $source,
        array $config,
    ): void {
        if (!$config['columns'] && !$config['schema'] && $source->isSliced()) {
            throw new InvalidOutputException(
                sprintf('Sliced file "%s" columns specification missing.', $source->getSourceName()),
            );
        }
    }

    private function validateMappingDestination(array $config): void
    {
        try {
            new MappingDestination($config['destination']);
        } catch (InvalidArgumentException $e) {
            throw new InvalidOutputException(sprintf(
                'Failed to resolve valid destination. "%s" is not a valid table ID.',
                $config['destination'],
            ), 0, $e);
        }
    }
}
