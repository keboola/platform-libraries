<?php

namespace Keboola\OutputMapping\Tests\Writer\Helper;

use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Tests\Writer\CreateBranchTrait;
use Keboola\OutputMapping\Writer\Helper\DestinationRewriter;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use PHPUnit\Framework\TestCase;

class DestinationRewriterTest extends TestCase
{
    use CreateBranchTrait;

    private function getConfig()
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
            new ClientOptions(STORAGE_API_URL, STORAGE_API_TOKEN_MASTER, $branchId, null, null, null, 1),
        );
    }

    public function testRewriteBranch()
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

    public function testRewriteNoBranch()
    {
        $clientWrapper = $this->getClientWrapper(null);
        $config = $this->getConfig();
        $expectedConfig = DestinationRewriter::rewriteDestination($config, $clientWrapper);
        self::assertEquals('in.c-main.table', $expectedConfig['destination']);
        unset($expectedConfig['destination']);
        unset($config['destination']);
        self::assertEquals($config, $expectedConfig);
    }

    public function testRewriteInvalidName()
    {
        $clientWrapper = $this->getClientWrapper(null);
        $branchId = $this->createBranch($clientWrapper, 'dev-123');
        $clientWrapper = $this->getClientWrapper($branchId);
        $config = $this->getConfig();
        $config['destination'] = 'in.c-main-table';
        self::expectExceptionMessage('Invalid destination: "in.c-main-table"');
        self::expectException(InvalidOutputException::class);
        DestinationRewriter::rewriteDestination($config, $clientWrapper);
    }
}
