<?php

declare(strict_types=1);

namespace Keboola\ConfigurationVariablesResolver;

use Keboola\ServiceClient\ServiceClient;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\VaultApiClient\ApiClientConfiguration as VaultVariablesApiClientConfiguration;
use Keboola\VaultApiClient\Variables\VariablesApiClient;
use Psr\Log\LoggerInterface;

readonly class UnifiedConfigurationResolverFactory
{
    public function __construct(
        private ServiceClient $serviceClient,
        private VaultVariablesApiClientConfiguration $vaultVariablesApiClientConfiguration,
        private LoggerInterface $logger,
    ) {
    }

    /**
     * Creates a new instance of UnifiedConfigurationResolver.
     */
    public function createResolver(
        ClientWrapper $clientWrapper,
        ?string $variableValuesId = null,
        ?array $variableValuesData = null,
    ): UnifiedConfigurationResolver {
        $token = $clientWrapper->getToken()->getTokenValue();
        assert($token !== '');

        $vaultVariablesApiClient = new VariablesApiClient(
            baseUrl: $this->serviceClient->getVaultUrl(),
            token: $token,
            configuration: $this->vaultVariablesApiClientConfiguration,
        );

        $sharedCodeResolver = new SharedCodeResolver($clientWrapper, $this->logger);
        $variablesResolver = VariablesResolver::create(
            $clientWrapper,
            $vaultVariablesApiClient,
            $this->logger,
        );

        $branchId = $clientWrapper->getBranchId();
        assert($branchId !== '');

        return new UnifiedConfigurationResolver(
            $sharedCodeResolver,
            $variablesResolver,
            $branchId,
            $variableValuesId === '' ? null : $variableValuesId,
            $variableValuesData,
        );
    }
}
