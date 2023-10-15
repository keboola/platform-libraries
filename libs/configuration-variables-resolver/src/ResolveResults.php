<?php

declare(strict_types=1);

namespace Keboola\ConfigurationVariablesResolver;

class ResolveResults
{
    /**
     * @param array<string|int, string> $replacedVariablesValues
     */
    public function __construct(
        public readonly array $configuration,
        public readonly array $replacedVariablesValues,
    ) {
    }
}
