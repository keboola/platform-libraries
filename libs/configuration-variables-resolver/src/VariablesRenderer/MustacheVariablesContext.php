<?php

declare(strict_types=1);

namespace Keboola\ConfigurationVariablesResolver\VariablesRenderer;

class MustacheVariablesContext
{
    /** @var array<non-empty-string, true>  */
    private array $replacedVariables = [];

    /** @var array<non-empty-string, true>  */
    private array $missingVariables = [];

    /**
     * @param array<non-empty-string, string> $values
     */
    public function __construct(
        private readonly array $values,
    ) {
    }

    /**
     * @param non-empty-string $name
     */
    public function __isset(string $name): bool
    {
        if (isset($this->values[$name])) {
            return true;
        }

        $this->missingVariables[$name] = true;
        return false;
    }

    /**
     * @param non-empty-string $name
     */
    public function __get(string $name): string|self
    {
        $this->replacedVariables[$name] = true;
        return $this->values[$name];
    }

    /**
     * @return list<non-empty-string>
     */
    public function getReplacedVariables(): array
    {
        return array_map(strval(...), array_keys($this->replacedVariables)); // @phpstan-ignore-line
    }

    /**
     * @return list<non-empty-string>
     */
    public function getMissingVariables(): array
    {
        return array_map(strval(...), array_keys($this->missingVariables)); // @phpstan-ignore-line
    }
}
