<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Table\Options;

use Keboola\InputMapping\Exception\TableNotFoundException;
use Keboola\InputMapping\State\InputTableStateList;

class RewrittenInputTableOptions extends InputTableOptions
{
    private array $tableInfo;

    public function __construct(array $definition, string $source, int $sourceBranchId, array $tableInfo)
    {
        parent::__construct($definition);
        $this->definition['source'] = $source;
        $this->definition['source_branch_id'] = $sourceBranchId;
        $this->tableInfo = $tableInfo;
    }

    public function getTableInfo(): array
    {
        return $this->tableInfo;
    }

    /**
     * @return array{
     *     sourceBranchId?: int,
     *     columns?: array<string>,
     *     changedSince?: string,
     *     whereColumn?: string,
     *     whereValues?: array<string>,
     *     whereOperator?: string,
     *     limit?: integer,
     *     overwrite?: bool,
     *     fileType?: string,
     * }
     */
    public function getStorageApiExportOptions(InputTableStateList $states): array
    {
        $exportOptions = [];
        if ($this->getSourceBranchId() !== null) {
            // practically, sourceBranchId should never be null, but i'm not able to make that statically safe and
            // passing null causes application error in connection, so here is a useless condition.
            $exportOptions['sourceBranchId'] = $this->getSourceBranchId();
        }
        if (count($this->definition['column_types'])) {
            $exportOptions['columns'] = $this->getColumnNamesFromTypes();
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
                } catch (TableNotFoundException) {
                    // intentionally blank
                }
            } else {
                $exportOptions['changedSince'] = (string) $this->definition['changed_since'];
            }
        }
        if (isset($this->definition['where_column']) && count($this->definition['where_values'])) {
            $exportOptions['whereColumn'] = (string) $this->definition['where_column'];
            $exportOptions['whereValues'] = (array) $this->definition['where_values'];
            $exportOptions['whereOperator'] = (string) $this->definition['where_operator'];
        }
        if (isset($this->definition['limit'])) {
            $exportOptions['limit'] = (int) $this->definition['limit'];
        }
        if ($this->getFileType() !== null) {
            $exportOptions['fileType'] = $this->getFileType();
        }
        $exportOptions['overwrite'] = (bool) $this->definition['overwrite'];
        return $exportOptions;
    }
}
