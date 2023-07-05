<?php

declare(strict_types=1);

namespace Keboola\ConfigurationVariablesResolver\Tests;

use Keboola\ConfigurationVariablesResolver\Exception\UserException;
use Keboola\ConfigurationVariablesResolver\VariablesRenderer\RegexRenderer;
use Keboola\ConfigurationVariablesResolver\VariablesRenderer\RenderResults;
use Keboola\ConfigurationVariablesResolver\VariablesResolver;
use Keboola\ConfigurationVariablesResolver\VariablesResolver\ConfigurationVariablesResolver;
use Keboola\ConfigurationVariablesResolver\VariablesResolver\VaultVariablesResolver;
use Monolog\Handler\TestHandler;
use Monolog\Logger;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;

class VariablesResolverTest extends TestCase
{
    private readonly TestHandler $logsHandler;
    private readonly LoggerInterface $logger;

    public function setUp(): void
    {
        parent::setUp();

        $this->logsHandler = new TestHandler();
        $this->logger = new Logger('tests', [$this->logsHandler]);
    }

    public function testResolveVariables(): void
    {
        $configuration = [
            'variables_id' => '123',
            'variables_values_id' => '456',
            'parameters' => [
                'some_parameter' => '{{ foo }} {{ vault.foo }}',
            ],
        ];

        $configurationAfterResolveVault = [
            'variables_id' => '123',
            'variables_values_id' => '456',
            'parameters' => [
                'some_parameter' => '{{ foo }} vault',
            ],
        ];

        $configurationAfterResolveConfiguration = [
            'variables_id' => '123',
            'variables_values_id' => '456',
            'parameters' => [
                'some_parameter' => 'config vault',
            ],
        ];

        $vaultVariablesResolver = $this->createMock(VaultVariablesResolver::class);
        $vaultVariablesResolver->expects(self::once())
            ->method('resolveVariables')
            ->with($configuration, 'branch-id')
            ->willReturn(new RenderResults(
                $configurationAfterResolveVault,
                ['vault.foo'],
                [],
            ))
        ;

        $configurationVariablesResolver = $this->createMock(ConfigurationVariablesResolver::class);
        $configurationVariablesResolver->expects(self::once())
            ->method('resolveVariables')
            ->with($configurationAfterResolveVault, '123', ['456'])
            ->willReturn(new RenderResults(
                $configurationAfterResolveConfiguration,
                ['foo'],
                [],
            ))
        ;

        $resolver = new VariablesResolver(
            $configurationVariablesResolver,
            $vaultVariablesResolver,
            $this->logger,
        );

        $newConfiguration = $resolver->resolveVariables($configuration, 'branch-id', '123', ['456']);
        self::assertSame($configurationAfterResolveConfiguration, $newConfiguration);

        self::assertTrue($this->logsHandler->hasInfoThatContains('Replaced values for variables: vault.foo, foo'));
    }


    public function testResolveMissingVariables(): void
    {
        $configuration = [
            'variables_id' => '123',
            'variables_values_id' => '456',
            'parameters' => [
                'some_parameter' => '{{ foo }} {{ vault.foo }}',
            ],
        ];

        $configurationAfterResolveVault = [
            'variables_id' => '123',
            'variables_values_id' => '456',
            'parameters' => [
                'some_parameter' => '{{ foo }} {{ vault.foo }}',
            ],
        ];

        $configurationAfterResolveConfiguration = [
            'variables_id' => '123',
            'variables_values_id' => '456',
            'parameters' => [
                'some_parameter' => ' {{ vault.foo }}',
            ],
        ];

        $vaultVariablesResolver = $this->createMock(VaultVariablesResolver::class);
        $vaultVariablesResolver->expects(self::once())
            ->method('resolveVariables')
            ->with($configuration, 'branch-id')
            ->willReturn(new RenderResults(
                $configurationAfterResolveVault,
                [],
                ['vault.foo'],
            ))
        ;

        $configurationVariablesResolver = $this->createMock(ConfigurationVariablesResolver::class);
        $configurationVariablesResolver->expects(self::once())
            ->method('resolveVariables')
            ->with($configurationAfterResolveVault, '123', ['456'])
            ->willReturn(new RenderResults(
                $configurationAfterResolveConfiguration,
                [],
                ['foo'],
            ))
        ;

        $resolver = new VariablesResolver(
            $configurationVariablesResolver,
            $vaultVariablesResolver,
            $this->logger,
        );

        $this->expectException(UserException::class);
        $this->expectExceptionMessage('Missing values for placeholders: vault.foo, foo');

        $resolver->resolveVariables($configuration, 'branch-id', '123', ['456']);
    }
}
