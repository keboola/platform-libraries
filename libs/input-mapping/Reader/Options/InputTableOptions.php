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
        // column type is required either for all columns or it mustn't be present in any column
        $typesCount = 0;
        foreach ($this->definition['columns'] as $column) {
            if (!empty($column['type'])) {
                $typesCount++;
            }
        }
        if (($typesCount !== 0) && ($typesCount !== count($this->definition['columns']))) {
            throw new InvalidInputException(
                'Columns must be either specified as array of strings or as array of objects with type, but not both.'
            );
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

    /**
     * @return array
     */
    public function getColumnNames()
    {
        $colNames = [];
        if (isset($this->definition['columns'])) {
            foreach ($this->definition['columns'] as $column) {
                $colNames[] = $column['source'];
            }
        }
        return $colNames;
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
        return $exportOptions;
    }

    /**
     * @return array
     */
    public function getStorageApiLoadOptions(InputTableStateList $states)
    {
        $exportOptions = [];
        if (isset($this->definition['columns']) && count($this->definition['columns'])) {
            /** Columns are formally always specified in extended format here, if the type is specified it's either
             *  specified for all columns (it was originally in extended format) or for no columns (it was originally
             *  a simple list). We check the first column (because all are same) to see if the types are specified
             * or not.
             */
            if (!empty($this->definition['columns'][0]['type'])) {
                $exportOptions['columns'] = $this->definition['columns'];
            } else {
                $exportOptions['columns'] = $this->getColumnNames();
            }
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
        return $exportOptions;
    }
}
