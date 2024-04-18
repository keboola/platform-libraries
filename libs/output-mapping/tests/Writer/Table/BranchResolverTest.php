<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Table;

use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Tests\AbstractTestCase;
use Keboola\OutputMapping\Tests\Writer\CreateBranchTrait;
use Keboola\OutputMapping\Writer\Table\BranchResolver;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\ClientOptions;

class BranchResolverTest extends AbstractTestCase
{
    use CreateBranchTrait;

    public function testRewriteBranchSourceMainBranch(): void
    {
        $branchResolver = new BranchResolver($this->clientWrapper);

        $config = [
            'destination' => 'out.c-main.table',
        ];

        $config = $branchResolver->rewriteBranchSource($config);

        self::assertEquals('out.c-main.table', $config['destination']);
    }

    public function testRewriteBranchSourceDevelopmentBranch(): void
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
            'destination' => 'out.c-dev.table',
        ];

        $config = $branchResolver->rewriteBranchSource($config);

        self::assertStringMatchesFormat('out.c-%d-dev.table', $config['destination']);
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
}
