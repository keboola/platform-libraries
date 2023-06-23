<?php

declare(strict_types=1);

namespace Keboola\ConfigurationVariablesResolver\Tests\VariablesLoader;

use Keboola\ConfigurationVariablesResolver\VariablesLoader\VaultVariablesLoader;
use Keboola\VaultApiClient\Variables\Model\Variable;
use Keboola\VaultApiClient\Variables\VariablesApiClient;
use PHPUnit\Framework\TestCase;

class VaultVariablesLoaderTest extends TestCase
{
    public function testLoadVariables(): void
    {
        $vaultClient = $this->createMock(VariablesApiClient::class);
        $vaultClient->expects(self::once())
            ->method('listMergedVariablesForBranch')
            ->with('123')
            ->willReturn([
                new Variable('hash1', 'key1', 'val1', false, []),
                new Variable('hash2', 'key2', 'val2', false, []),
            ])
        ;

        $loader = new VaultVariablesLoader($vaultClient);
        $variables = $loader->loadVariables('123');

        self::assertSame(
            [
                'key1' => 'val1',
                'key2' => 'val2',
            ],
            $variables,
        );
    }
}
