<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Table\Options;

use Keboola\InputMapping\Configuration\Table;
use Keboola\InputMapping\Exception\InvalidInputException;
use Keboola\InputMapping\Exception\TableNotFoundException;
use Keboola\InputMapping\State\InputTableStateList;

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
     * Returns the parsed (snake_case) input-mapping configuration for the workspace input-mapping-load
     * endpoint, which accepts the Configuration\Table payload unchanged.
     *
     * The only value the endpoint cannot interpret is the "adaptive" changed_since marker — it is
     * specific to input mapping and depends on the previous run's state — so we resolve it here to the
     * source's last import date (as a unix timestamp). When the source has no recorded state yet
     * (e.g. first run), the marker is dropped so the whole table is loaded.
     *
     * @return array<string, mixed>
     */
    public function getStorageApiWorkspaceLoadConfiguration(InputTableStateList $states): array
    {
        if (!empty($this->definition['days'])) {
            throw new InvalidInputException(
                'Days option is not supported on workspace, use changed_since instead.',
            );
        }

        $definition = $this->definition;
        if (($definition['changed_since'] ?? '') === self::ADAPTIVE_INPUT_MAPPING_VALUE) {
            $unixTimestamp = $this->resolveAdaptiveChangedSince($states);
            if ($unixTimestamp !== null) {
                $definition['changed_since'] = (string) $unixTimestamp;
            } else {
                unset($definition['changed_since']);
            }
        }
        return $definition;
    }

    /**
     * Resolves the "adaptive" changed_since marker to the source's last import date as a unix timestamp.
     * Returns null when the source has no recorded state yet (e.g. first run), in which case callers omit
     * the filter and load the whole table.
     */
    private function resolveAdaptiveChangedSince(InputTableStateList $states): ?int
    {
        try {
            $lastImportDateString = $states
                ->getTable($this->getSource())
                ->getLastImportDate();
        } catch (TableNotFoundException) {
            return null;
        }

        // converting to unix timestamp https://keboolaglobal.slack.com/archives/C054VSPFVST/p1723555870048739?thread_ts=1723531121.814779&cid=C054VSPFVST
        // strtotime() returns false on failure; 0 (the unix epoch) is a valid timestamp, so compare strictly.
        $unixTimestamp = strtotime($lastImportDateString);
        if ($unixTimestamp === false) {
            throw new InvalidInputException(
                sprintf(
                    'Invalid lastImportDate value "%s" for table "%s". '
                    . 'This value cannot be converted to a valid timestamp.',
                    $lastImportDateString,
                    $this->getSource(),
                ),
            );
        }
        return $unixTimestamp;
    }

    public function getLoadType(): ?string
    {
        return isset($this->definition['load_type']) ? (string) $this->definition['load_type'] : null;
    }

    public function keepInternalTimestampColumn(): bool
    {
        return (bool) $this->definition['keep_internal_timestamp_column'];
    }

    public function getSourceBranchId(): ?int
    {
        return isset($this->definition['source_branch_id']) ? (int) $this->definition['source_branch_id'] : null;
    }

    public function getFileType(): ?string
    {
        return isset($this->definition['file_type']) ? (string) $this->definition['file_type'] : null;
    }
}
