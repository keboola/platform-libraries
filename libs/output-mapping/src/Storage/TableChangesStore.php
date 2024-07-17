<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Storage;

use Keboola\OutputMapping\Mapping\MappingFromConfigurationSchemaColumn;

class TableChangesStore
{
    /** @var MappingFromConfigurationSchemaColumn[] $missingColumns */
    private array $missingColumns = [];

    public function hasMissingColumns(): bool
    {
        return count($this->missingColumns) > 0;
    }

    public function getMissingColumns(): array
    {
        return $this->missingColumns;
    }

    public function addMissingColumn(MappingFromConfigurationSchemaColumn $missingColumn): void
    {
        $this->missingColumns[] = $missingColumn;
    }
}
