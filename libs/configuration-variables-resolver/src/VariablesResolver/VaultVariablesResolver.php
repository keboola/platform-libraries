<?php

declare(strict_types=1);

namespace Keboola\ConfigurationVariablesResolver\VariablesResolver;

use Keboola\ConfigurationVariablesResolver\VariablesRenderer\RegexRenderer;
use Keboola\ConfigurationVariablesResolver\VariablesRenderer\RenderResults;
use Keboola\VaultApiClient\Variables\VariablesApiClient;

class VaultVariablesResolver
{
    public function __construct(
        private readonly VariablesApiClient $variablesApiClient,
        private readonly RegexRenderer $renderer,
    ) {
    }

    /**
     * @param non-empty-string $branchId
     */
    public function resolveVariables(array $configuration, string $branchId): RenderResults
    {
        $loadVariables = function () use ($branchId): array {
            $variables = $this->variablesApiClient->listScopedVariablesForBranch($branchId);

            $keyVal = [];
            foreach ($variables as $variable) {
                $keyVal[$variable->key] = $variable->value;
            }

            return $keyVal;
        };

        return $this->renderer->renderVariables(
            $configuration,
            'vault',
            $loadVariables,
        );
    }
}
