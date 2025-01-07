<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Helper;

use Generator;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Tests\AbstractTestCase;
use Keboola\OutputMapping\Tests\Needs\NeedsDevBranch;
use Keboola\OutputMapping\Writer\Helper\DestinationRewriter;
use Keboola\StorageApi\BranchAwareClient;
use Keboola\StorageApiBranch\ClientWrapper;

class DestinationRewriterTest extends AbstractTestCase
{
    private const MAPPING_CONFIG = [
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

    #[NeedsDevBranch]
    public function testRewriteBranch(): void
    {
        $this->initClient($this->devBranchId);

        $config = self::MAPPING_CONFIG;
        $expectedConfig = DestinationRewriter::rewriteDestination($config, $this->clientWrapper);
        self::assertEquals(sprintf('in.c-%s-main.table', $this->devBranchId), $expectedConfig['destination']);
        unset($expectedConfig['destination']);
        unset($config['destination']);
        self::assertEquals($config, $expectedConfig);
    }

    public function testRewriteNoBranch(): void
    {
        $config = self::MAPPING_CONFIG;
        $expectedConfig = DestinationRewriter::rewriteDestination($config, $this->clientWrapper);
        self::assertEquals('in.c-main.table', $expectedConfig['destination']);
        unset($expectedConfig['destination']);
        unset($config['destination']);
        self::assertEquals($config, $expectedConfig);
    }

    #[NeedsDevBranch]
    public function testRewriteInvalidName(): void
    {
        $this->initClient($this->devBranchId);

        $config = self::MAPPING_CONFIG;
        $config['destination'] = 'in.c-main-table';
        $this->expectExceptionMessage('Invalid destination: "in.c-main-table"');
        $this->expectException(InvalidOutputException::class);
        DestinationRewriter::rewriteDestination($config, $this->clientWrapper);
    }

    /** @dataProvider rewriteConfigProvider  */
    public function testRewritePrefixes(array $config, ?string $branchId, string $expectedDestination): void
    {
        $clientMock = $this->createMock(BranchAwareClient::class);

        $clientWrapper = $this->createMock(ClientWrapper::class);
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
