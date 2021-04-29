<?php

declare(strict_types=1);

namespace Keboola\ConfigurationVariablesResolver;

class VariablesContext
{
    private array $missingVariables;
    private array $values;

    public function __construct(array $configurationRow)
    {
        $this->values = [];
        foreach ($configurationRow['values'] as $row) {
            $this->values[$row['name']] = $row['value'];
        }
        $this->missingVariables = [];
    }

    public function __isset(string $name): bool
    {
        if (isset($this->values[$name])) {
            return true;
        }
        $this->missingVariables[] = $name;
        return false;
    }

    public function __get(string $name): string
    {
        return $this->values[$name];
    }

    public function getMissingVariables(): array
    {
        return $this->missingVariables;
    }
}
