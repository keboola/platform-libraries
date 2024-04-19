<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Table;

use Generator;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Tests\AbstractTestCase;
use Keboola\OutputMapping\Tests\Writer\CreateBranchTrait;
use Keboola\OutputMapping\Writer\Table\BranchResolver;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\ClientOptions;

class BranchResolverTest extends AbstractTestCase
{
    use CreateBranchTrait;

    /**
     * @dataProvider rewriteConfigMainBranchProvider
     */
    public function testRewriteBranchSourceMainBranch(array $config, string $expectedDestination): void
    {
        $branchResolver = new BranchResolver($this->clientWrapper);

        $config = $branchResolver->rewriteBranchSource($config);

        self::assertEquals($expectedDestination, $config['destination']);
    }

    /**
     * @dataProvider rewriteConfigBranchStorageProvider
     */
    public function testRewriteBranchSourceInBranchStorage(array $config, string $expectedDestination): void
    {
        $clientWrapper = new ClientWrapper(
            new ClientOptions(
                url: (string) getenv('STORAGE_API_URL'),
                token: (string) getenv('STORAGE_API_TOKEN_MASTER'),
                useBranchStorage: true, // this is the important part
            ),
        );
        $branchId = $this->createBranch($clientWrapper, self::class);
        $clientWrapper = new ClientWrapper(
            new ClientOptions(
                url: (string) getenv('STORAGE_API_URL'),
                token: (string) getenv('STORAGE_API_TOKEN'),
                branchId: $branchId,
                useBranchStorage: true,
            ),
        );

        $branchResolver = new BranchResolver($clientWrapper);

        $config = $branchResolver->rewriteBranchSource($config);

        self::assertStringMatchesFormat($expectedDestination, $config['destination']);
    }

    /**
     * @dataProvider rewriteConfigDevelopmentBranchProvider
     */
    public function testRewriteBranchSourceDevelopmentBranch(array $config, string $expectedDestination): void
    {
        $clientWrapper = new ClientWrapper(
            new ClientOptions(
                (string) getenv('STORAGE_API_URL'),
                (string) getenv('STORAGE_API_TOKEN_MASTER'),
                null,
            ),
        );
        $branchName = self::class;
        $branchId = $this->createBranch($clientWrapper, $branchName);

        // set it to use a branch
        $this->initClient($branchId);

        $branchResolver = new BranchResolver($this->clientWrapper);

        $config = $branchResolver->rewriteBranchSource($config);

        self::assertStringMatchesFormat($expectedDestination, $config['destination']);
    }

    public function testErrorRewriteBranchSourceInvalidDestination(): void
    {
        $clientWrapper = new ClientWrapper(
            new ClientOptions(
                (string) getenv('STORAGE_API_URL'),
                (string) getenv('STORAGE_API_TOKEN_MASTER'),
                null,
            ),
        );
        $branchName = self::class;
        $branchId = $this->createBranch($clientWrapper, $branchName);

        // set it to use a branch
        $this->initClient($branchId);

        $branchResolver = new BranchResolver($this->clientWrapper);

        $config = [
            'destination' => 'out.c-dev.table.error',
        ];

        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage('Invalid destination: "out.c-dev.table.error"');
        $branchResolver->rewriteBranchSource($config);
    }

    public function rewriteConfigMainBranchProvider(): Generator
    {
        yield 'without prefix' => [
            'config' => [
                'destination' => 'in.main.table',
            ],
            'expectedDestination' => 'in.main.table',
        ];
        yield 'with prefix' => [
            'config' => [
                'destination' => 'in.c-main.table',
            ],
            'expectedDestination' => 'in.c-main.table',
        ];
    }

    public function rewriteConfigDevelopmentBranchProvider(): Generator
    {
        yield 'without prefix' => [
            'config' => [
                'destination' => 'in.c-main.table',
            ],
            'expectedDestination' => 'in.c-%s-main.table',
        ];
        yield 'with prefix' => [
            'config' => [
                'destination' => 'in.main.table',
            ],
            'expectedDestination' => 'in.%s-main.table',
        ];
    }

    public function rewriteConfigBranchStorageProvider(): Generator
    {
        yield 'without prefix' => [
            'config' => [
                'destination' => 'in.c-main.table',
            ],
            'expectedDestination' => 'in.c-main.table',
        ];
        yield 'with prefix' => [
            'config' => [
                'destination' => 'in.main.table',
            ],
            'expectedDestination' => 'in.main.table',
        ];
    }
}
