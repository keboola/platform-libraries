<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Mapping;

abstract class AbstractMappingFromConfigurationDeleteWhereFilter
{
    public function __construct(
        protected readonly array $mapping,
    ) {
    }

    public function getColumn(): string
    {
        return $this->mapping['column'];
    }

    public function getOperator(): string
    {
        return $this->mapping['operator'];
    }
}
