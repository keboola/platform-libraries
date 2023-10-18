<?php

declare(strict_types=1);

namespace Keboola\ConfigurationVariablesResolver\Tests\VariablesResolver;

use Keboola\ConfigurationVariablesResolver\VariablesRenderer\RegexRenderer;
use Keboola\ConfigurationVariablesResolver\VariablesResolver\VaultVariablesResolver;
use Keboola\VaultApiClient\Variables\Model\Variable;
use Keboola\VaultApiClient\Variables\VariablesApiClient;
use PHPUnit\Framework\TestCase;

class VaultVariablesResolverTest extends TestCase
{
    public function testResolveVariables(): void
    {
        $configuration = [
            'parameters' => [
                'foo' => '{{ vault.foo }}',
            ],
        ];

        $vaultApiClient = $this->createMock(VariablesApiClient::class);
        $vaultApiClient->expects(self::once())
            ->method('listScopedVariablesForBranch')
            ->with('branch-id')
            ->willReturn([
                new Variable('hash1', 'foo', 'bar', [], ['branchId' => 'branch-id']),
            ])
        ;

        $resolver = new VaultVariablesResolver($vaultApiClient, new RegexRenderer());
        $results = $resolver->resolveVariables($configuration, 'branch-id');

        self::assertSame([
            'parameters' => [
                'foo' => 'bar',
            ],
        ], $results->configuration);
        self::assertSame(['vault.foo' => 'bar'], $results->replacedVariablesValues);
        self::assertSame([], $results->missingVariables);
    }
}
