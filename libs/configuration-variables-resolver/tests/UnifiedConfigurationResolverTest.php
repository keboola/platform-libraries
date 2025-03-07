<?php

declare(strict_types=1);

namespace Keboola\ConfigurationVariablesResolver\Tests;

use Keboola\ConfigurationVariablesResolver\SharedCodeResolver;
use Keboola\ConfigurationVariablesResolver\UnifiedConfigurationResolver;
use Keboola\ConfigurationVariablesResolver\VariablesResolver;
use Keboola\StorageApi\BranchAwareClient;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\VaultApiClient\Variables\Model\Variable;
use Keboola\VaultApiClient\Variables\VariablesApiClient;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

class UnifiedConfigurationResolverTest extends TestCase
{
    public function testResolveConfigurationWithSharedCodeAndVault(): void
    {
        $variablesApiClientMock = $this->createMock(VariablesApiClient::class);
        $variablesApiClientMock->method('listScopedVariablesForBranch')
            ->with('branch-id')
            ->willReturn([
                new Variable(
                    'non-empty-string',
                    'vaultVariable',
                    'vaultVariableValue',
                    [],
                    [],
                ),
            ])
        ;

        $branchAwareClientMock = $this->createMock(BranchAwareClient::class);
        $branchAwareClientMock->method('apiGet')
            ->willReturnCallback(static fn($url) => match ($url) {
                'components/keboola.variables/configs/variablesId' => ['configuration' => [
                    'variables' => [
                        [
                            'name' => 'variable',
                            'type' => 'string',
                        ],
                    ],
                ]],
                'components/keboola.variables/configs/variablesId/rows/variablesRowId' => ['configuration' => [
                    'values' => [
                        [
                            'name' => 'variable',
                            'value' => 'variableValue',
                        ],
                    ],
                ]],
                'components/keboola.shared-code/configs/sharedCodeId/rows/sharedCodeRowId' => ['configuration' => [
                    'code_content' => [
                        '{{ variable }}',
                    ],
                ]],
                default => null,
            })
        ;

        $clientWrapperMock = $this->createMock(ClientWrapper::class);
        $clientWrapperMock->method('getBranchId')
            ->willReturn('branch-id');
        $clientWrapperMock->method('getBranchClient')
            ->willReturn($branchAwareClientMock);

        $unifiedVariablesResolver = $this->createResolver(
            $variablesApiClientMock,
            $clientWrapperMock,
        );

        $results = $unifiedVariablesResolver->resolveConfiguration(
            [
                'variables_id' => 'variablesId',
                'variables_values_id' => 'variablesRowId',
                'parameters' => [
                    'parameter_with_variables' => '{{ variable }} {{ vault.vaultVariable }}',
                    'parameter_with_shared_code' => ['{{ sharedCodeRowId }}'], // resolved in arrays only
                ],
                'shared_code_id' => 'sharedCodeId',
                'shared_code_row_ids' => [
                    'sharedCodeRowId',
                ],
            ],
        );

        self::assertEquals(
            [
                'variables_id' => 'variablesId',
                'variables_values_id' => 'variablesRowId',
                'parameters' => [
                    'parameter_with_variables' => 'variableValue vaultVariableValue',
                    'parameter_with_shared_code' => ['variableValue'], // shared code must be resolved before variables
                ],
                'shared_code_id' => 'sharedCodeId',
                'shared_code_row_ids' => [
                    'sharedCodeRowId',
                ],
            ],
            $results->configuration, // we are working with only one configuration
        );
    }

    public function testResolveConfigurationVariableIdOverride(): void
    {
        $variablesApiClientMock = $this->createMock(VariablesApiClient::class);

        $branchAwareClientMock = $this->createMock(BranchAwareClient::class);
        $branchAwareClientMock->method('apiGet')
            ->willReturnCallback(static fn($url) => match ($url) {
                'components/keboola.variables/configs/variablesId' => ['configuration' => [
                    'variables' => [
                        [
                            'name' => 'variable',
                            'type' => 'string',
                        ],
                    ],
                ]],
                'components/keboola.variables/configs/variablesId/rows/variablesRowIdOne' => ['configuration' => [
                    'values' => [
                        [
                            'name' => 'variable',
                            'value' => 'variableValueOne',
                        ],
                    ],
                ]],
                'components/keboola.variables/configs/variablesId/rows/variablesRowIdTwo' => ['configuration' => [
                    'values' => [
                        [
                            'name' => 'variable',
                            'value' => 'variableValueTwo',
                        ],
                    ],
                ]],
                default => null,
            })
        ;

        $clientWrapperMock = $this->createMock(ClientWrapper::class);
        $clientWrapperMock->method('getBranchId')
            ->willReturn('branch-id');
        $clientWrapperMock->method('getBranchClient')
            ->willReturn($branchAwareClientMock);

        $unifiedVariablesResolver = $this->createResolver(
            $variablesApiClientMock,
            $clientWrapperMock,
            variableValuesId: 'variablesRowIdTwo',
        );

        $results = $unifiedVariablesResolver->resolveConfiguration(
            [
                'variables_id' => 'variablesId',
                'variables_values_id' => 'variablesRowIdOne',
                'parameters' => [
                    'parameter_with_variables' => '{{ variable }}',
                ],
            ],
        );

        self::assertEquals(
            [
                'variables_id' => 'variablesId',
                'variables_values_id' => 'variablesRowIdOne',
                'parameters' => [
                    'parameter_with_variables' => 'variableValueTwo',
                ],
            ],
            $results->configuration, // we are working with only one configuration
        );
    }

    public function testResolveConfigurationVariableDataOverride(): void
    {
        $variablesApiClientMock = $this->createMock(VariablesApiClient::class);

        $branchAwareClientMock = $this->createMock(BranchAwareClient::class);
        $branchAwareClientMock->method('apiGet')
            ->willReturn(['configuration' => []])
        ;

        $clientWrapperMock = $this->createMock(ClientWrapper::class);
        $clientWrapperMock->method('getBranchId')
            ->willReturn('branch-id');
        $clientWrapperMock->method('getBranchClient')
            ->willReturn($branchAwareClientMock);

        $unifiedVariablesResolver = $this->createResolver(
            $variablesApiClientMock,
            $clientWrapperMock,
            variableValuesData: [
                'values' => [
                    [
                        'name' => 'inlinedVariable',
                        'value' => 'variableValue',
                    ],
                ],
            ],
        );

        $results = $unifiedVariablesResolver->resolveConfiguration(
            [
                'variables_id' => 'variablesId', // must be present even for inlined variables
                'parameters' => [
                    'parameter_with_variables' => '{{ inlinedVariable }}',
                ],
            ],
        );

        self::assertEquals(
            [
                'variables_id' => 'variablesId',
                'parameters' => [
                    'parameter_with_variables' => 'variableValue',

                ],
            ],
            $results->configuration, // we are working with only one configuration
        );
    }

    /**
     * @param non-empty-string|null $variableValuesId
     */
    private function createResolver(
        VariablesApiClient $vaultVariablesApiClient,
        ClientWrapper $clientWrapper,
        ?string $variableValuesId = null,
        ?array $variableValuesData = null,
    ): UnifiedConfigurationResolver {
        $sharedCodeResolver = new SharedCodeResolver($clientWrapper, new NullLogger);
        $variablesResolver = VariablesResolver::create(
            $clientWrapper,
            $vaultVariablesApiClient,
            new NullLogger,
        );

        return new UnifiedConfigurationResolver(
            $sharedCodeResolver,
            $variablesResolver,
            /** @phpstan-ignore-next-line */
            $clientWrapper->getBranchId(),
            $variableValuesId,
            $variableValuesData,
        );
    }
}
