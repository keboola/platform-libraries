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
                    'param' => 'key1: {{ key1 }}, key2: {{ key2 }}, key3: {{ key3 }}',
                ],
            ],
            null,
            null,
        );

        self::assertSame(
            [
                'parameters' => [
                    'param' => 'key1: val1-conf, key2: val2-conf, key3: val3-vault',
                ],
            ],
            $configuration,
        );
    }

    public function testResolveVariablesWithNumericKey(): void
    {
        // PHP converts numeric keys in arrays to integers, it can cause problems if not handled properly

        $configurationVariablesLoader = $this->createMock(ConfigurationVariablesLoader::class);
        $configurationVariablesLoader
            ->method('loadVariables')
            ->willReturn([
               '123' => '321',
               '789' => '987',
            ])
        ;

        $vaultVariablesLoader = $this->createMock(VaultVariablesLoader::class);
        $vaultVariablesLoader
            ->method('loadVariables')
            ->willReturn([
                '456' => '654',
                '789' => '147',
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
                    'param' => 'key1: {{ 123 }}, key2: {{ 456 }}, key3: {{ 789 }}',
                ],
            ],
            null,
            null,
        );

        self::assertSame(
            [
                'parameters' => [
                    'param' => 'key1: 321, key2: 654, key3: 987',
                ],
            ],
            $configuration,
        );
    }
}
