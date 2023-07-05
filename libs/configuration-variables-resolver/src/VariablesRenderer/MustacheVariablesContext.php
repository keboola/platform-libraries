<?php

declare(strict_types=1);

namespace Keboola\ConfigurationVariablesResolver\VariablesRenderer;

class MustacheVariablesContext
{
    /** @var array<non-empty-string, string|self>  */
    private readonly array $values;

    /** @var array<non-empty-string, true>  */
    private array $replacedVariables = [];

    /** @var array<non-empty-string, true>  */
    private array $missingVariables = [];

    /**
     * @param array<non-empty-string, string|array> $values
     */
    public function __construct(array $values)
    {
        $normalizedValues = [];
        foreach ($values as $name => $value) {
            if (is_array($value)) {
                $normalizedValues[$name] = new self($value);
            } else {
                $normalizedValues[$name] = $value;
            }
        }

        $this->values = $normalizedValues;
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

    public function getReplacedVariables(): array
    {
        $variables = [];
        foreach ($this->replacedVariables as $name => $true) {
            $value = $this->values[$name];
            if ($value instanceof self) {
                $variables = array_merge(
                    $variables,
                    array_map(
                        fn(string $variable) => $name . '.' . $variable,
                        $value->getReplacedVariables(),
                    ),
                );
            } else {
                $variables[] = (string) $name;
            }
        }

        return $variables;
    }

    public function getMissingVariables(): array
    {
        $variables = array_map(strval(...), array_keys($this->missingVariables));
        foreach ($this->values as $name => $value) {
            if ($value instanceof self) {
                $variables = array_merge(
                    $variables,
                    array_map(
                        fn(string $variable) => $name . '.' . $variable,
                        $value->getMissingVariables(),
                    ),
                );
            }
        }

        return $variables;
    }
}
