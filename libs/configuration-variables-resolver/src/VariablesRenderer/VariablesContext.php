<?php

declare(strict_types=1);

namespace Keboola\ConfigurationVariablesResolver\VariablesRenderer;

class VariablesContext
{
    private array $replacedVariables = [];
    private array $missingVariables = [];

    /**
     * @param array<non-empty-string, string> $values
     */
    public function __construct(
        private readonly array $values,
    ) {
    }

    public function __isset(string $name): bool
    {
        if (isset($this->values[$name])) {
            return true;
        }

        $this->missingVariables[$name] = true;
        return false;
    }

    public function __get(string $name): string
    {
        $this->replacedVariables[$name] = true;
        return $this->values[$name];
    }

    public function getReplacedVariables(): array
    {
        return array_keys($this->replacedVariables);
    }

    public function getMissingVariables(): array
    {
        return array_keys($this->missingVariables);
    }
}
