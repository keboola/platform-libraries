<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Mapping;

use Keboola\OutputMapping\Configuration\Table\Configuration;
use Keboola\OutputMapping\Writer\Helper\RestrictedColumnsHelper;
use Keboola\OutputMapping\Writer\Table\MappingDestination;
use Keboola\OutputMapping\Writer\Table\Source\SourceInterface;

class MappingFromProcessedConfiguration
{
    private MappingDestination $destination;
    private MappingFromRawConfigurationAndPhysicalDataWithManifest $source;
    private array $mapping;

    public function __construct(
        array $mapping,
        MappingFromRawConfigurationAndPhysicalDataWithManifest $source,
    ) {
        $this->destination = new MappingDestination($mapping['destination']);
        unset($mapping['destination']);

        $this->mapping = (new Configuration())->parse([$mapping]);
        $this->source = $source;
    }

    public function isSliced(): bool
    {
        return $this->source->isSliced();
    }

    public function getSourceName(): string
    {
        return $this->source->getSourceName();
    }

    public function getWorkspaceId(): string
    {
        return $this->source->getWorkspaceId();
    }

    public function getDataObject(): string
    {
        return $this->source->getDataObject();
    }

    public function getDestination(): MappingDestination
    {
        return $this->destination;
    }

    public function getDelimiter(): string
    {
        return $this->mapping['delimiter'];
    }

    public function getEnclosure(): string
    {
        return $this->mapping['enclosure'];
    }

    public function getDeleteWhereColumn(): ?string
    {
        if (!isset($this->mapping['delete_where_column'])) {
            return null;
        }
        if ($this->mapping['delete_where_column'] === '') {
            return null;
        }
        return $this->mapping['delete_where_column'];
    }

    public function getDeleteWhereValues(): array
    {
        return $this->mapping['delete_where_values'];
    }

    public function getDeleteWhereOperator(): string
    {
        return $this->mapping['delete_where_operator'];
    }

    public function getPrimaryKey(): array
    {
        return $this->mapping['primary_key'];
    }

    public function hasWriteAlways(): bool
    {
        return $this->mapping['write_always'] ?? false;
    }

    public function hasColumns(): bool
    {
        return !empty($this->getColumns());
    }

    public function hasColumnMetadata(): bool
    {
        return !empty($this->getColumnMetadata());
    }

    public function hasSchemaColumnMetadata(): bool
    {
        if ($this->getSchema() === null) {
            return false;
        }
        foreach ($this->getSchema() as $item) {
            if ($item->hasMetadata()) {
                return true;
            }
        }
        return false;
    }

    public function isIncremental(): bool
    {
        return $this->mapping['incremental'];
    }

    public function getColumns(): array
    {
        return $this->mapping['columns'] ?
            RestrictedColumnsHelper::removeRestrictedColumnsFromColumns($this->mapping['columns']) :
            [];
    }

    public function getColumnMetadata(): array
    {
        return $this->mapping['column_metadata'] ?
            RestrictedColumnsHelper::removeRestrictedColumnsFromColumnMetadata($this->mapping['column_metadata']) :
            [];
    }

    public function getPathName(): string
    {
        return $this->source->getPathname();
    }

    public function getTags(): array
    {
        return $this->mapping['tags'] ?? [];
    }

    public function hasDistributionKey(): bool
    {
        return !empty($this->mapping['distribution_key']);
    }

    public function getDistributionKey(): array
    {
        return $this->mapping['distribution_key'] ?? [];
    }

    public function hasMetadata(): bool
    {
        return !empty($this->mapping['metadata']);
    }

    public function getMetadata(): array
    {
        return $this->mapping['metadata'] ?? [];
    }

    public function hasTableMetadata(): bool
    {
        return !empty($this->getTableMetadata());
    }

    public function getTableMetadata(): array
    {
        $metadata = $this->mapping['table_metadata'] ?? [];
        if (isset($this->mapping['description'])) {
            $metadata['KBC.description'] = $this->mapping['description'];
        }
        return $metadata;
    }

    /**
     * @return class-string<SourceInterface>
     */
    public function getItemSourceClass(): string
    {
        return $this->source->getItemSourceClass();
    }

    /** @return null|MappingFromConfigurationSchemaColumn[] */
    public function getSchema(): ?array
    {
        $schema = $this->mapping['schema'] ?
            RestrictedColumnsHelper::removeRestrictedColumnsFromSchema($this->mapping['schema']) :
            [];

        return $schema ? array_map(fn($v) => new MappingFromConfigurationSchemaColumn($v), $schema) : null;
    }

    public function hasHeader(): bool
    {
        return $this->mapping['has_header'] ?? false;
    }
}
