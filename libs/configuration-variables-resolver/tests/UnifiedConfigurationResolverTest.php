<?php

declare(strict_types=1);

namespace Keboola\ConfigurationVariablesResolver\Tests;

use Keboola\ConfigurationVariablesResolver\ResolveResults;
use Keboola\ConfigurationVariablesResolver\SharedCodeResolver;
use Keboola\ConfigurationVariablesResolver\UnifiedConfigurationResolver;
use Keboola\ConfigurationVariablesResolver\VariablesResolver;
use PHPUnit\Framework\TestCase;

class UnifiedConfigurationResolverTest extends TestCase
{
    /**
     * @param non-empty-string|null $variableValuesId
     * @dataProvider resolverDataProvider
     */
    public function testResolver(
        array $configuration,
        ?string $variableValuesId,
        ?array $variableValuesData,
        array $expectedVariablesResolverCallParams,
    ): void {
        $sharedCodeResolver = $this->createMock(SharedCodeResolver::class);
        $variablesResolver = $this->createMock(VariablesResolver::class);

        $sharedCodeResolver
            ->expects($this->once())
            ->method('resolveSharedCode')
            ->with($configuration)
            ->willReturn(['config' => 'shared-code-resolved']);

        $expectedResult = new ResolveResults(['resolved' => true], []);
        $variablesResolver
            ->expects($this->once())
            ->method('resolveVariables')
            ->with(...$expectedVariablesResolverCallParams)
            ->willReturn($expectedResult);

        $resolver = new UnifiedConfigurationResolver(
            sharedCodeResolver: $sharedCodeResolver,
            variablesResolver: $variablesResolver,
            branchId: 'test-branch-id',
            variableValuesId: $variableValuesId,
            variableValuesData: $variableValuesData,
        );

        $result = $resolver->resolveConfiguration($configuration);

        $this->assertSame($expectedResult, $result);
    }

    public function resolverDataProvider(): iterable
    {
        yield 'with variable values id' => [
            'configuration' => ['config' => 'value'],
            'variableValuesId' => 'variable-values-id',
            'variableValuesData' => null,
            'expectedVariablesResolverCallParams' => [
                ['config' => 'shared-code-resolved'],
                'test-branch-id',
                'variable-values-id',
                null,
            ],
        ];

        yield 'with variable values data' => [
            'configuration' => ['config' => 'value'],
            'variableValuesId' => null,
            'variableValuesData' => ['values' => [['name' => 'testVar', 'value' => 'testValue']]],
            'expectedVariablesResolverCallParams' => [
                ['config' => 'shared-code-resolved'],
                'test-branch-id',
                null,
                ['values' => [['name' => 'testVar', 'value' => 'testValue']]],
            ],
        ];

        yield 'without variable params' => [
            'configuration' => ['config' => 'value'],
            'variableValuesId' => null,
            'variableValuesData' => null,
            'expectedVariablesResolverCallParams' => [
                ['config' => 'shared-code-resolved'],
                'test-branch-id',
                null,
                null,
            ],
        ];
    }
}
