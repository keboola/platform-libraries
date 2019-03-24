<?php

namespace Keboola\InputMapping\Reader\Options;

use Keboola\InputMapping\Exception\InvalidInputException;
use Keboola\InputMapping\Reader\State\Exception\TableNotFoundException;
use Keboola\InputMapping\Reader\State\InputTablesState;

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
    public function getColumns()
    {
        if (isset($this->definition['columns'])) {
            return $this->definition['columns'];
        }
        return [];
    }

    /**
     * @return array
     */
    public function getStorageApiExportOptions(InputTablesState $states)
    {
        $exportOptions = [];
        if (isset($this->definition['columns']) && count($this->definition['columns'])) {
            $exportOptions['columns'] = $this->definition['columns'];
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
}
