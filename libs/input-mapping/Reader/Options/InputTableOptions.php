<?php

namespace Keboola\InputMapping\Reader\Options;

use Keboola\InputMapping\Exception\InvalidInputException;
use Keboola\InputMapping\Reader\State\Exception\TableNotFoundException;
use Keboola\InputMapping\Reader\State\InputTableStateList;

class InputTableOptions
{
    const ADAPTIVE_INPUT_MAPPING_VALUE = 'adaptive';

    /**
     * @var array
     */
    private $definition;

    public function __construct(array $configuration)
    {
        if (!empty($configuration['changed_since']) && !empty($configuration['days'])) {
            throw new InvalidInputException('Cannot set both parameters "days" and "changed_since".');
        }
        $tableConfiguration = new \Keboola\InputMapping\Configuration\Table();
        $this->definition = $tableConfiguration->parse(['table' => $configuration]);
        $colNamesFromTypes = [];
        foreach ($this->definition['column_types'] as $column) {
            $colNamesFromTypes[] = $column['source'];
        }
        $this->validateColumns($colNamesFromTypes);
        if (empty($this->definition['columns']) && !empty($colNamesFromTypes)) {
            $this->definition['columns'] = $colNamesFromTypes;
        }
    }

    private function validateColumns(array $colNamesFromTypes)
    {
        // if both columns and column_types are entered, verify that the columns listed do match
        if ($this->definition['columns'] && $this->definition['column_types']) {
            $diff = array_diff($this->definition['columns'], $colNamesFromTypes);
            if ($diff) {
                throw new InvalidInputException(sprintf(
                    'Both "columns" and "column_types" are specified, "columns" field contains surplus columns: "%s".',
                    implode($diff, ', ')
                ));
            }
            $diff = array_diff($colNamesFromTypes, $this->definition['columns']);
            if ($diff) {
                throw new InvalidInputException(sprintf(
                    'Both "columns" and "column_types" are specified, "column_types" field contains surplus columns: "%s".',
                    implode($diff, ', ')
                ));
            }
        }
    }

    /**
     * @return array
     */
    public function getDefinition()
    {
        return $this->definition;
    }

    /**
     * @return string
     */
    public function getSource()
    {
        return $this->definition['source'];
    }

    public function setSource($newSource)
    {
        $this->definition['source'] = $newSource;
    }

    /**
     * @return string
     */
    public function getDestination()
    {
        if (isset($this->definition['destination'])) {
            return $this->definition['destination'];
        }
        return '';
    }

    public function getOverwrite()
    {
        return $this->definition['overwrite'];
    }

    /**
     * @return array
     */
    public function getColumnNames()
    {
        if (isset($this->definition['columns'])) {
            return $this->definition['columns'];
        }
        return [];
    }

    /**
     * @return array
     */
    public function getStorageApiExportOptions(InputTableStateList $states)
    {
        $exportOptions = [];
        if (isset($this->definition['columns']) && count($this->definition['columns'])) {
            $exportOptions['columns'] = $this->getColumnNames();
        }
        if (!empty($this->definition['days'])) {
            $exportOptions['changedSince'] = "-{$this->definition["days"]} days";
        }
        if (!empty($this->definition['changed_since'])) {
            if ($this->definition['changed_since'] === self::ADAPTIVE_INPUT_MAPPING_VALUE) {
                try {
                    $exportOptions['changedSince'] = $states
                        ->getTable($this->getSource())
                        ->getLastImportDate();
                } catch (TableNotFoundException $e) {
                    // intentionally blank
                }
            } else {
                $exportOptions['changedSince'] = $this->definition['changed_since'];
            }
        }
        if (isset($this->definition['where_column']) && count($this->definition['where_values'])) {
            $exportOptions['whereColumn'] = $this->definition['where_column'];
            $exportOptions['whereValues'] = $this->definition['where_values'];
            $exportOptions['whereOperator'] = $this->definition['where_operator'];
        }
        if (isset($this->definition['limit'])) {
            $exportOptions['limit'] = $this->definition['limit'];
        }
        $exportOptions['overwrite'] = $this->definition['overwrite'];
        return $exportOptions;
    }

    private function getColumnTypes()
    {
        if ($this->definition['column_types']) {
            $ret = [];
            foreach ($this->definition['column_types'] as $column_type) {
                $item = [
                    'source' => $column_type['source'],
                    'type' => $column_type['type'],
                ];
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
     * @return array
     */
    public function getStorageApiLoadOptions(InputTableStateList $states)
    {
        $exportOptions = [];
        if ($this->definition['column_types']) {
            $exportOptions['columns'] = $this->getColumnTypes();
        } elseif ($this->definition['columns']) {
            $exportOptions['columns'] = $this->getColumnNames();
        }
        if (!empty($this->definition['days'])) {
            throw new InvalidInputException(
                'Days option is not supported on workspace, use changed_since instead.'
            );
        }
        if (!empty($this->definition['changed_since'])) {
            if ($this->definition['changed_since'] === self::ADAPTIVE_INPUT_MAPPING_VALUE) {
                throw new InvalidInputException(
                    'Adaptive input mapping is not supported on input mapping to workspace.'
                );
            } else {
                if (strtotime($this->definition['changed_since']) === false) {
                    throw new InvalidInputException(
                        sprintf('Error parsing changed_since expression "%s".', $this->definition['changed_since'])
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
}
