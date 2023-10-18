<?php

declare(strict_types=1);

namespace Keboola\ConfigurationVariablesResolver;

use Keboola\ConfigurationVariablesResolver\Exception\UserException;
use Keboola\ConfigurationVariablesResolver\VariablesRenderer\MustacheRenderer;
use Keboola\ConfigurationVariablesResolver\VariablesRenderer\RegexRenderer;
use Keboola\ConfigurationVariablesResolver\VariablesResolver\ConfigurationVariablesResolver;
use Keboola\ConfigurationVariablesResolver\VariablesResolver\VaultVariablesResolver;
use Keboola\StorageApi\Components;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\VaultApiClient\Variables\VariablesApiClient;
use Psr\Log\LoggerInterface;

class VariablesResolver
{
    public function __construct(
        private readonly ConfigurationVariablesResolver $configurationVariablesResolver,
        private readonly VaultVariablesResolver $vaultVariablesResolver,
        private readonly LoggerInterface $logger,
    ) {
    }

    public static function create(
        ClientWrapper $clientWrapper,
        VariablesApiClient $variablesApiClient,
        LoggerInterface $logger,
    ): self {
        return new self(
            new ConfigurationVariablesResolver(
                new ComponentsClientHelper(
                    new Components($clientWrapper->getBranchClient()),
                ),
                new MustacheRenderer(),
                $logger,
            ),
            new VaultVariablesResolver(
                $variablesApiClient,
                new RegexRenderer(),
            ),
            $logger,
        );
    }

    /**
     * @param array{variables_id?: string|null, variables_values_id?: string|null} $configuration
     * @param non-empty-string $branchId
     * @param non-empty-string|null $variableValuesId
     * @param array{values?: list<array{name: scalar, value: scalar}>|null}|null $variableValuesData
     */
    public function resolveVariables(
        array $configuration,
        string $branchId,
        ?string $variableValuesId,
        ?array $variableValuesData,
    ): ResolveResults {

        $vaultResult = $this->vaultVariablesResolver->resolveVariables(
            $configuration,
            $branchId,
        );

        $configurationResult = $this->configurationVariablesResolver->resolveVariables(
            $vaultResult->configuration,
            $variableValuesId,
            $variableValuesData,
        );

        $missingVariables = array_merge($vaultResult->missingVariables, $configurationResult->missingVariables);
        $replacedVariables = array_merge(
            array_keys($vaultResult->replacedVariablesValues),
            array_keys($configurationResult->replacedVariablesValues),
        );

        if (count($missingVariables) > 0) {
            throw new UserException(sprintf(
                'Missing values for placeholders: %s',
                implode(', ', $missingVariables),
            ));
        }

        if (count($replacedVariables) > 0) {
            $this->logger->info(sprintf(
                'Replaced values for variables: %s',
                implode(', ', $replacedVariables),
            ));
        }

        return new ResolveResults(
            $configurationResult->configuration,
            array_merge(
                $vaultResult->replacedVariablesValues,
                $configurationResult->replacedVariablesValues,
            ),
        );
    }
}
