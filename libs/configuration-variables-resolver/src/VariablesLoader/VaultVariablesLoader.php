<?php

declare(strict_types=1);

namespace Keboola\ConfigurationVariablesResolver\VariablesLoader;

use Keboola\VaultApiClient\Variables\VariablesApiClient;

class VaultVariablesLoader
{
    public function __construct(
        private readonly VariablesApiClient $variablesApiClient,
    ) {
    }

    /**
     * @param non-empty-string $branchId
     * @return array<non-empty-string, string>
     */
    public function loadVariables(string $branchId): array
    {
        $variables = $this->variablesApiClient->listMergedVariablesForBranch($branchId);

        $indexed = [];
        foreach ($variables as $variable) {
            $indexed[$variable->key] = $variable->value;
        }
        return $indexed;
    }
}
