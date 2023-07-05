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
        $variables = $this->variablesApiClient->listMergedVariablesForBranch($branchId);

        $keyVal = [];
        foreach ($variables as $variable) {
            $keyVal[$variable->key] = $variable->value;
        }

        return $this->renderer->renderVariables(
            $configuration,
            'vault',
            $keyVal,
        );
    }
}
