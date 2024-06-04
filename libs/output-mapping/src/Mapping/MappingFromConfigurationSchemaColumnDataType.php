<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Mapping;

use Keboola\OutputMapping\Exception\InvalidOutputException;

class MappingFromConfigurationSchemaColumnDataType
{
    public function __construct(private readonly array $mapping)
    {
    }

    public function getTypeName(?string $backend = null): string
    {
        if ($backend === null) {
            return $this->getBaseTypeName();
        }

        try {
            return $this->getBackendTypeName($backend);
        } catch (InvalidOutputException $e) {
            return $this->getBaseTypeName();
        }
    }

    public function getLength(?string $backend = null): ?string
    {
        if ($backend === null) {
            return $this->getBaseLength();
        }

        try {
            return $this->getBackendLength($backend);
        } catch (InvalidOutputException $e) {
            return $this->getBaseLength();
        }
    }

    public function getDefaultValue(?string $backend = null): ?string
    {
        if ($backend === null) {
            return $this->getBaseDefaultValue();
        }

        try {
            return $this->getBackendDefaultValue($backend);
        } catch (InvalidOutputException $e) {
            return $this->getBaseDefaultValue();
        }
    }

    public function getBaseTypeName(): string
    {
        return $this->mapping['base']['type'];
    }

    public function getBaseLength(): ?string
    {
        return $this->mapping['base']['length'] ?? null;
    }

    public function getBaseDefaultValue(): ?string
    {
        return $this->mapping['base']['default'] ?? null;
    }

    public function getBackendTypeName(string $backend): string
    {
        if (!isset($this->mapping[$backend])) {
            throw new InvalidOutputException(sprintf('Backend "%s" not found in mapping.', $backend));
        }
        return $this->mapping[$backend]['type'];
    }

    public function getBackendLength(string $backend): ?string
    {
        if (!isset($this->mapping[$backend])) {
            throw new InvalidOutputException(sprintf('Backend "%s" not found in mapping.', $backend));
        }
        return $this->mapping[$backend]['length'] ?? null;
    }

    public function getBackendDefaultValue(string $backend): ?string
    {
        if (!isset($this->mapping[$backend])) {
            throw new InvalidOutputException(sprintf('Backend "%s" not found in mapping.', $backend));
        }
        return $this->mapping[$backend]['default'] ?? null;
    }
}
