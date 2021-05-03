<?php

declare(strict_types=1);

namespace Keboola\ConfigurationVariablesResolver;

class SharedCodeContext
{
    private array $values = [];

    public function pushValue(string $key, string $value): void
    {
        $this->values[$key] = $value;
    }

    public function getKeys(): array
    {
        return array_keys($this->values);
    }

    public function __isset(string $name): bool
    {
        return true;
    }

    public function __get(string $name): string
    {
        if (isset($this->values[$name])) {
            return $this->values[$name];
        } else {
            return '{{ ' . $name . ' }}';
        }
    }
}
