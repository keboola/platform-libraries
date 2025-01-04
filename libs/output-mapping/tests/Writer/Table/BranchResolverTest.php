<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Table;

use Generator;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Tests\AbstractTestCase;
use Keboola\OutputMapping\Tests\Needs\NeedsDevBranch;
use Keboola\OutputMapping\Writer\Table\BranchResolver;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\ClientOptions;

class BranchResolverTest extends AbstractTestCase
{
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
    #[NeedsDevBranch]
    public function testRewriteBranchSourceInBranchStorage(array $config, string $expectedDestination): void
    {
        $clientWrapper = new ClientWrapper(
            new ClientOptions(
                url: (string) getenv('STORAGE_API_URL'),
                token: (string) getenv('STORAGE_API_TOKEN'),
                branchId: $this->devBranchId,
                useBranchStorage: true, // this is the important part
            ),
        );

        $branchResolver = new BranchResolver($clientWrapper);

        $config = $branchResolver->rewriteBranchSource($config);

        self::assertStringMatchesFormat($expectedDestination, $config['destination']);
    }

    /**
     * @dataProvider rewriteConfigDevelopmentBranchProvider
     */
    #[NeedsDevBranch]
    public function testRewriteBranchSourceDevelopmentBranch(array $config, string $expectedDestination): void
    {
        $this->initClient($this->devBranchId);

        $branchResolver = new BranchResolver($this->clientWrapper);

        $config = $branchResolver->rewriteBranchSource($config);

        self::assertStringMatchesFormat($expectedDestination, $config['destination']);
    }

    #[NeedsDevBranch]
    public function testErrorRewriteBranchSourceInvalidDestination(): void
    {
        $this->initClient($this->devBranchId);

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
