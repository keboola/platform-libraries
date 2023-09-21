<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Helper;

use Generator;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Tests\Writer\CreateBranchTrait;
use Keboola\OutputMapping\Writer\Helper\DestinationRewriter;
use Keboola\StorageApi\BranchAwareClient;
use Keboola\StorageApi\Client;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use PHPUnit\Framework\TestCase;

class DestinationRewriterTest extends TestCase
{
    use CreateBranchTrait;

    private function getConfig(): array
    {
        return [
            'source' => 'some-table.csv',
            'destination' => 'in.c-main.table',
            'primary_key' => ['id', 'name'],
            'columns' => ['id', 'name', 'description', 'foo', 'bar'],
            'delimiter' => ',',
            'enclosure' => '',
            'metadata' => [
                [
                    'key' => 'foo',
                    'bar' => 'value',
                ],
            ],
        ];
    }

    protected function getClientWrapper(?string $branchId): ClientWrapper
    {
        return new ClientWrapper(
            new ClientOptions(
                (string) getenv('STORAGE_API_URL'),
                (string) getenv('STORAGE_API_TOKEN_MASTER'),
                $branchId,
                null,
                null,
                null,
                1,
            ),
        );
    }

    public function testRewriteBranch(): void
    {
        $clientWrapper = $this->getClientWrapper(null);
        $branchId = $this->createBranch($clientWrapper, 'dev 123');
        $clientWrapper = $this->getClientWrapper($branchId);

        $config = $this->getConfig();
        $expectedConfig = DestinationRewriter::rewriteDestination($config, $clientWrapper);
        self::assertEquals(sprintf('in.c-%s-main.table', $branchId), $expectedConfig['destination']);
        unset($expectedConfig['destination']);
        unset($config['destination']);
        self::assertEquals($config, $expectedConfig);
    }

    public function testRewriteNoBranch(): void
    {
        $clientWrapper = $this->getClientWrapper(null);
        $config = $this->getConfig();
        $expectedConfig = DestinationRewriter::rewriteDestination($config, $clientWrapper);
        self::assertEquals('in.c-main.table', $expectedConfig['destination']);
        unset($expectedConfig['destination']);
        unset($config['destination']);
        self::assertEquals($config, $expectedConfig);
    }

    public function testRewriteInvalidName(): void
    {
        $clientWrapper = $this->getClientWrapper(null);
        $branchId = $this->createBranch($clientWrapper, self::class);
        $clientWrapper = $this->getClientWrapper($branchId);
        $config = $this->getConfig();
        $config['destination'] = 'in.c-main-table';
        self::expectExceptionMessage('Invalid destination: "in.c-main-table"');
        self::expectException(InvalidOutputException::class);
        DestinationRewriter::rewriteDestination($config, $clientWrapper);
    }

    /** @dataProvider rewriteConfigProvider  */
    public function testRewritePrefixes(array $config, ?string $branchId, string $expectedDestination): void
    {
        $clientMock = self::createMock(BranchAwareClient::class);

        $clientWrapper = self::createMock(ClientWrapper::class);
        $clientWrapper->method('getBranchClient')->willReturn($clientMock);
        // let's say 456 is branchId of default branch
        $clientWrapper->method('getBranchId')->willReturn($branchId ?? '456');
        $clientWrapper->method('isDevelopmentBranch')->willReturn($branchId !== null);

        $expectedConfig = DestinationRewriter::rewriteDestination($config, $clientWrapper);
        self::assertEquals($expectedDestination, $expectedConfig['destination']);
        unset($expectedConfig['destination']);
        unset($config['destination']);
        self::assertEquals($config, $expectedConfig);
    }

    public function rewriteConfigProvider(): Generator
    {
        yield 'without prefix without branch' => [
            'config' => [
                'source' => 'some-table.csv',
                'destination' => 'in.main.table',
                'primary_key' => ['id', 'name'],
                'columns' => ['id', 'name', 'description', 'foo', 'bar'],
                'delimiter' => ',',
                'enclosure' => '',
                'metadata' => [
                    [
                        'key' => 'foo',
                        'bar' => 'value',
                    ],
                ],
            ],
            'branchId' => null,
            'expectedDestination' => 'in.main.table',
        ];
        yield 'with prefix without branch' => [
            'config' => [
                'source' => 'some-table.csv',
                'destination' => 'in.c-main.table',
                'primary_key' => ['id', 'name'],
                'columns' => ['id', 'name', 'description', 'foo', 'bar'],
                'delimiter' => ',',
                'enclosure' => '',
                'metadata' => [
                    [
                        'key' => 'foo',
                        'bar' => 'value',
                    ],
                ],
            ],
            'branchId' => null,
            'expectedDestination' => 'in.c-main.table',
        ];
        yield 'with prefix with branch' => [
            'config' => [
                'source' => 'some-table.csv',
                'destination' => 'in.c-main.table',
                'primary_key' => ['id', 'name'],
                'columns' => ['id', 'name', 'description', 'foo', 'bar'],
                'delimiter' => ',',
                'enclosure' => '',
                'metadata' => [
                    [
                        'key' => 'foo',
                        'bar' => 'value',
                    ],
                ],
            ],
            'branchId' => '123',
            'expectedDestination' => 'in.c-123-main.table',
        ];
        yield 'without prefix with branch' => [
            'config' => [
                'source' => 'some-table.csv',
                'destination' => 'in.main.table',
                'primary_key' => ['id', 'name'],
                'columns' => ['id', 'name', 'description', 'foo', 'bar'],
                'delimiter' => ',',
                'enclosure' => '',
                'metadata' => [
                    [
                        'key' => 'foo',
                        'bar' => 'value',
                    ],
                ],
            ],
            'branchId' => '123',
            'expectedDestination' => 'in.123-main.table',
        ];
    }
}
