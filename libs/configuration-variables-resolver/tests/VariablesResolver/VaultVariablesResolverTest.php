<?php

declare(strict_types=1);

namespace Keboola\ConfigurationVariablesResolver\Tests\VariablesResolver;

use Keboola\ConfigurationVariablesResolver\VariablesRenderer\RegexRenderer;
use Keboola\ConfigurationVariablesResolver\VariablesRenderer\RenderResults;
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
            ->method('listMergedVariablesForBranch')
            ->with('branch-id')
            ->willReturn([
                new Variable('hash1', 'foo', 'bar', false, ['branchId' => 'branch-id']),
            ])
        ;

        $renderResults = new RenderResults(
            [
                'parameters' => [
                    'foo' => 'bar',
                ],
            ],
            ['vault.foo'],
            [],
        );

        $renderer = $this->createMock(RegexRenderer::class);
        $renderer->expects(self::once())
            ->method('renderVariables')
            ->with($configuration, 'vault', ['foo' => 'bar'])
            ->willReturn($renderResults)
        ;

        $resolver = new VaultVariablesResolver($vaultApiClient, $renderer);
        $results = $resolver->resolveVariables($configuration, 'branch-id');
        self::assertSame($renderResults, $results);
    }
}
