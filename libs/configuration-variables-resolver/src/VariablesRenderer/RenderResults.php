<?php

declare(strict_types=1);

namespace Keboola\ConfigurationVariablesResolver\VariablesRenderer;

class RenderResults
{
    /**
     * @var array
     * @deprecated
     */
    public readonly array $replacedVariables;
    /**
     * @param array<string|int, string> $replacedVariablesValues
     * @param list<non-empty-string> $missingVariables
     */
    public function __construct(
        public readonly array $configuration,
        array $replacedVariables,
        public readonly array $replacedVariablesValues,
        public readonly array $missingVariables,
    ) {
        $this->replacedVariables = $replacedVariables;
    }
}
