<?php

declare(strict_types=1);

namespace Keboola\ConfigurationVariablesResolver;

use Keboola\ConfigurationVariablesResolver\VariablesLoader\ConfigurationVariablesLoader;
use Keboola\ConfigurationVariablesResolver\VariablesLoader\VaultVariablesLoader;
use Keboola\ConfigurationVariablesResolver\VariablesRenderer\VariablesRenderer;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\VaultApiClient\Variables\VariablesApiClient;
use Psr\Log\LoggerInterface;

class VariableResolver
{
    public function __construct(
        private readonly ConfigurationVariablesLoader $configurationVariablesLoader,
        private readonly VaultVariablesLoader $vaultVariablesLoader,
        private readonly VariablesRenderer $variablesRenderer,
    ) {
    }

    public static function create(
        ClientWrapper $clientWrapper,
        VariablesApiClient $variablesApiClient,
        LoggerInterface $logger,
    ): self {
        $componentsClientHelper = new ComponentsClientHelper($clientWrapper);

        return new self(
            new ConfigurationVariablesLoader($componentsClientHelper, $logger),
            new VaultVariablesLoader($variablesApiClient),
            new VariablesRenderer($logger),
        );
    }

    /**
     * @param array{variables_id?: string|null, variables_values_id?: string|null} $configuration
     * @param non-empty-string $branchId
     * @param non-empty-string|null $variableValuesId
     * @param array{values?: list<array{name: scalar, value: scalar}>|null}|null $variableValuesData
     * @return array<non-empty-string, string>
     */
    public function resolveVariables(
        array $configuration,
        string $branchId,
        ?string $variableValuesId,
        ?array $variableValuesData,
    ): array {
        // !!! do not use array_merge() here as it can break numeric keys, array union is safe
        $variables =
            $this->configurationVariablesLoader->loadVariables($configuration, $variableValuesId, $variableValuesData) +
            $this->vaultVariablesLoader->loadVariables($branchId)
        ;

        return $this->variablesRenderer->renderVariables($configuration, $variables);
    }
}
