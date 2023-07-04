<?php

declare(strict_types=1);

namespace Keboola\ConfigurationVariablesResolver\Tests;

use Keboola\ConfigurationVariablesResolver\VariableResolver;
use Keboola\ConfigurationVariablesResolver\VariablesLoader\ConfigurationVariablesLoader;
use Keboola\ConfigurationVariablesResolver\VariablesLoader\VaultVariablesLoader;
use Keboola\ConfigurationVariablesResolver\VariablesRenderer\VariablesRenderer;
use Monolog\Handler\TestHandler;
use Monolog\Logger;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;

class VariableResolverTest extends TestCase
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
        $configurationVariablesLoader = $this->createMock(ConfigurationVariablesLoader::class);
        $configurationVariablesLoader
            ->method('loadVariables')
            ->willReturn([
                'key1' => 'val1-conf',
                'key2' => 'val2-conf',
            ])
        ;

        $vaultVariablesLoader = $this->createMock(VaultVariablesLoader::class);
        $vaultVariablesLoader
            ->method('loadVariables')
            ->willReturn([
                'key1' => 'val1-vault',
                'key3' => 'val3-vault',
            ])
        ;

        $resolver = new VariableResolver(
            $configurationVariablesLoader,
            $vaultVariablesLoader,
            new VariablesRenderer($this->logger),
        );
        $configuration = $resolver->resolveVariables(
            [
                'parameters' => [
                    'param' => 'global key1: {{ key1 }}, global key2: {{ key2 }}, ' .
                        'vault key1: {{ vault.key1 }}, vault key3: {{ vault.key3 }}',
                ],
            ],
            'branch-id',
            null,
            null,
        );

        self::assertSame(
            [
                'parameters' => [
                    'param' => 'global key1: val1-conf, global key2: val2-conf, ' .
                        'vault key1: val1-vault, vault key3: val3-vault',
                ],
            ],
            $configuration,
        );
    }
}
