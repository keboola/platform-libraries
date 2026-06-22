<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Mapping;

use Keboola\OutputMapping\Configuration\Table\Configuration;
use Keboola\OutputMapping\Configuration\Table\DeduplicationStrategy;
use Keboola\OutputMapping\Writer\Helper\RestrictedColumnsHelper;
use Keboola\OutputMapping\Writer\Table\MappingDestination;
use Keboola\OutputMapping\Writer\Table\Source\SourceType;

class MappingFromProcessedConfiguration
{
    private const DESCRIPTION_METADATA_KEY = 'KBC.description';

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
        return array_map('strval', $this->mapping['primary_key']);
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
        return RestrictedColumnsHelper::removeRestrictedColumnsFromColumns(
            $this->mapping['columns'] ? array_map('strval', $this->mapping['columns']) : [],
        );
    }

    /**
     * @return MappingColumnMetadata[]
     */
    public function getColumnMetadata(): array
    {
        $columnMetadataFromConfiguration = $this->mapping['column_metadata'] ?
            RestrictedColumnsHelper::removeRestrictedColumnsFromColumnMetadata($this->mapping['column_metadata']) :
            []
        ;

        $return = [];
        foreach ($columnMetadataFromConfiguration as $columnName => $metadata) {
            // description is applied through the table-definition endpoint, not as KBC.description metadata
            $metadata = array_values(array_filter(
                $metadata,
                fn(array $item): bool => ($item['key'] ?? null) !== self::DESCRIPTION_METADATA_KEY,
            ));
            if (!$metadata) {
                continue;
            }
            $return[] = new MappingColumnMetadata((string) $columnName, $metadata);
        }

        return $return;
    }

    /**
     * Column descriptions keyed by column name, sourced from the schema or the column metadata.
     * These are applied through the table-definition endpoint instead of KBC.description metadata.
     *
     * @return array<string, string>
     */
    public function getColumnDescriptions(): array
    {
        $result = [];

        $schema = $this->getSchema();
        if ($schema !== null) {
            foreach ($schema as $column) {
                $description = $column->getDescription();
                if ($description !== null) {
                    $result[$column->getName()] = $description;
                }
            }
            return $result;
        }

        $columnMetadata = $this->mapping['column_metadata'] ?
            RestrictedColumnsHelper::removeRestrictedColumnsFromColumnMetadata($this->mapping['column_metadata']) :
            [];
        foreach ($columnMetadata as $columnName => $metadata) {
            foreach ($metadata as $item) {
                if (($item['key'] ?? null) === self::DESCRIPTION_METADATA_KEY) {
                    $result[(string) $columnName] = (string) $item['value'];
                }
            }
        }

        return $result;
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
        // description is applied through the table-definition endpoint, not as KBC.description metadata
        unset($metadata[self::DESCRIPTION_METADATA_KEY]);
        return $metadata;
    }

    public function getTableDescription(): ?string
    {
        if (isset($this->mapping['description'])) {
            return $this->mapping['description'];
        }
        return $this->mapping['table_metadata'][self::DESCRIPTION_METADATA_KEY] ?? null;
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

    public function getItemSourceType(): SourceType
    {
        return $this->source->getSourceType();
    }

    /**
     * @return MappingFromConfigurationDeleteWhere[]|null
     */
    public function getDeleteWhere(): ?array
    {
        if (!isset($this->mapping['delete_where'])) {
            return null;
        }

        return array_map(
            function (array $deleteWhere): MappingFromConfigurationDeleteWhere {
                return new MappingFromConfigurationDeleteWhere($deleteWhere);
            },
            $this->mapping['delete_where'],
        );
    }

    public function getDeduplicationStrategy(): ?DeduplicationStrategy
    {
        $value = $this->mapping['deduplication_strategy'] ?? null;

        return $value !== null
            ? DeduplicationStrategy::from($value)
            : null;
    }

    public function getUnloadStrategy(): ?string
    {
        return $this->mapping['unload_strategy'] ?? null;
    }
}
