<?php

declare(strict_types=1);

namespace Keboola\ConfigurationVariablesResolver;

class SharedCodeContext
{
    private array $values = [];

    public function pushValue(string $key, array $value): void
    {
        $this->values[$key] = $value;
    }

    public function getKeys(): array
    {
        return array_keys($this->values);
    }

    public function __isset(string $name): bool
    {
        return isset($this->values[$name]);
    }

    public function __get(string $name): array
    {
        return $this->values[$name];
    }
}
