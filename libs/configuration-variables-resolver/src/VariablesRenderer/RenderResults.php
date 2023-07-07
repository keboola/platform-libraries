<?php

declare(strict_types=1);

namespace Keboola\ConfigurationVariablesResolver\VariablesRenderer;

class RenderResults
{
    /**
     * @param list<non-empty-string> $replacedVariables
     * @param list<non-empty-string> $missingVariables
     */
    public function __construct(
        public readonly array $configuration,
        public readonly array $replacedVariables,
        public readonly array $missingVariables,
    ) {
    }
}
