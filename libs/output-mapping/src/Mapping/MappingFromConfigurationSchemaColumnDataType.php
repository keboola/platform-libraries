<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Mapping;

use Keboola\OutputMapping\Exception\InvalidOutputException;

class MappingFromConfigurationSchemaColumnDataType
{
    public function __construct(private readonly array $mapping)
    {
    }

    public function getBaseType(): string
    {
        return $this->mapping['base']['type'];
    }

    public function getBaseLength(): ?string
    {
        return $this->mapping['base']['length'] ?? null;
    }

    public function getBaseDefault(): ?string
    {
        return $this->mapping['base']['default'] ?? null;
    }

    public function getTypeName(string $backend): string
    {
        if (!isset($this->mapping[$backend])) {
            throw new InvalidOutputException(sprintf('Backend "%s" not found in mapping.', $backend));
        }
        return $this->mapping[$backend]['type'];
    }

    public function getLength(string $backend): ?string
    {
        if (!isset($this->mapping[$backend])) {
            throw new InvalidOutputException(sprintf('Backend "%s" not found in mapping.', $backend));
        }
        return $this->mapping[$backend]['length'] ?? null;
    }

    public function getDefaultValue(string $backend): ?string
    {
        if (!isset($this->mapping[$backend])) {
            throw new InvalidOutputException(sprintf('Backend "%s" not found in mapping.', $backend));
        }
        return $this->mapping[$backend]['default'] ?? null;
    }
}
