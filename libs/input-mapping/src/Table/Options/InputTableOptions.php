<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Table\Options;

use Keboola\InputMapping\Configuration\Table;
use Keboola\InputMapping\Exception\InvalidInputException;
use Keboola\InputMapping\Exception\TableNotFoundException;
use Keboola\InputMapping\State\InputTableStateList;

/** @phpstan-type ColumnType = array{
 *     source: string,
 *     type?: string,
 *     destination?: string,
 *     length?: integer,
 *     nullable?: bool,
 *     convertEmptyValuesToNull?: bool,
 * }
*/
class InputTableOptions
{
    public const ADAPTIVE_INPUT_MAPPING_VALUE = 'adaptive';

    protected array $definition;

    public function __construct(array $configuration)
    {
        if (!empty($configuration['changed_since']) && !empty($configuration['days'])) {
            throw new InvalidInputException('Cannot set both parameters "days" and "changed_since".');
        }
        $tableConfiguration = new Table();
        $this->definition = $tableConfiguration->parse(['table' => $configuration]);
        $this->validateColumns();
        if (empty($this->definition['column_types'])) {
            foreach ($this->definition['columns'] as $column) {
                $this->definition['column_types'][] = ['source' => $column];
            }
        }
        $this->definition['columns'] = $this->getColumnNamesFromTypes();
    }

    private function validateColumns(): void
    {
        $colNamesFromTypes = $this->getColumnNamesFromTypes();
        // if both columns and column_types are entered, verify that the columns listed do match
        if ($this->definition['columns'] && $this->definition['column_types']) {
            $diff = array_diff($this->definition['columns'], $colNamesFromTypes);
            if ($diff) {
                throw new InvalidInputException(sprintf(
                    'Both "columns" and "column_types" are specified, "columns" field contains surplus columns: "%s".',
                    implode(', ', $diff),
                ));
            }
            $diff = array_diff($colNamesFromTypes, $this->definition['columns']);
            if ($diff) {
                throw new InvalidInputException(sprintf(
                    'Both "columns" and "column_types" are specified, ' .
                    '"column_types" field contains surplus columns: "%s".',
                    implode(', ', $diff),
                ));
            }
        }
    }

    /**
     * @return array
     */
    public function getDefinition(): array
    {
        return $this->definition;
    }

    public function getSource(): string
    {
        return $this->definition['source'];
    }

    public function getDestination(): string
    {
        if (isset($this->definition['destination'])) {
            return $this->definition['destination'];
        }
        return '';
    }

    public function getOverwrite(): bool
    {
        return (bool) $this->definition['overwrite'];
    }

    /**
     * @return array<string>
     */
    public function getColumnNamesFromTypes(): array
    {
        return array_column($this->definition['column_types'], 'source');
    }

    /**
     * @return array<int, ColumnType>
     */
    private function getColumnTypes(): array
    {
        if ($this->definition['column_types']) {
            $ret = [];
            foreach ($this->definition['column_types'] as $column_type) {
                $item = [
                    'source' => $column_type['source'],
                ];
                if (isset($column_type['type'])) {
                    $item['type'] = $column_type['type'];
                }
                if (isset($column_type['destination'])) {
                    $item['destination'] = $column_type['destination'];
                }
                if (isset($column_type['length'])) {
                    $item['length'] = $column_type['length'];
                }
                if (isset($column_type['nullable'])) {
                    $item['nullable'] = $column_type['nullable'];
                }
                if (isset($column_type['convert_empty_values_to_null'])) {
                    $item['convertEmptyValuesToNull'] = $column_type['convert_empty_values_to_null'];
                }
                $ret[] = $item;
            }
            return $ret;
        } else {
            return [];
        }
    }

    /**
     * @return array{
     *     columns?: array<int, ColumnType>,
     *     seconds?: integer,
     *     whereColumn?: string,
     *     whereValues?: array<string>,
     *     whereOperator?: string,
     *     rows?: integer,
     *     overwrite: bool,
     * }
     */
    public function getStorageApiLoadOptions(InputTableStateList $states): array
    {
        $exportOptions = [];
        if ($this->definition['column_types']) {
            $exportOptions['columns'] = $this->getColumnTypes();
        }
        if (!empty($this->definition['days'])) {
            throw new InvalidInputException(
                'Days option is not supported on workspace, use changed_since instead.',
            );
        }
        if (!empty($this->definition['changed_since'])) {
            if ($this->definition['changed_since'] === self::ADAPTIVE_INPUT_MAPPING_VALUE) {
                throw new InvalidInputException(
                    'Adaptive input mapping is not supported on input mapping to workspace.',
                );
            } else {
                if (strtotime($this->definition['changed_since']) === false) {
                    throw new InvalidInputException(
                        sprintf('Error parsing changed_since expression "%s".', $this->definition['changed_since']),
                    );
                }
                $exportOptions['seconds'] = time() - strtotime($this->definition['changed_since']);
            }
        }
        if (isset($this->definition['where_column']) && count($this->definition['where_values'])) {
            $exportOptions['whereColumn'] = $this->definition['where_column'];
            $exportOptions['whereValues'] = $this->definition['where_values'];
            $exportOptions['whereOperator'] = $this->definition['where_operator'];
        }
        if (isset($this->definition['limit'])) {
            $exportOptions['rows'] = $this->definition['limit'];
        }
        $exportOptions['overwrite'] = $this->definition['overwrite'];
        return $exportOptions;
    }

    public function isUseView(): bool
    {
        return (bool) $this->definition['use_view'];
    }

    public function keepInternalTimestampColumn(): bool
    {
        return (bool) $this->definition['keep_internal_timestamp_column'];
    }
}
