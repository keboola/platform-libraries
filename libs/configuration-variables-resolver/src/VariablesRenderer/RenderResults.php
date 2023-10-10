<?php

declare(strict_types=1);

namespace Keboola\ConfigurationVariablesResolver\VariablesRenderer;

class RenderResults
{
    /**
     * @param array<string|int, string> $replacedVariablesValues
     * @param list<non-empty-string> $missingVariables
     */
    public function __construct(
        public readonly array $configuration,
        public readonly array $replacedVariablesValues,
        public readonly array $missingVariables,
    ) {
    }
}
