<?php

namespace Keboola\OutputMapping\Tests\Writer\Helper;

use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Tests\Writer\CreateBranchTrait;
use Keboola\OutputMapping\Writer\Helper\DestinationRewriter;
use Keboola\StorageApi\Client;
use Keboola\StorageApiBranch\ClientWrapper;
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

    public function testRewriteBranch()
    {
        $clientWrapper = new ClientWrapper(
            new Client([
                'url' => STORAGE_API_URL,
                'token' => STORAGE_API_TOKEN_MASTER,
                'backoffMaxTries' => 1,
                'jobPollRetryDelay' => function () {
                    return 1;
                },
            ]),
            null,
            null
        );

        $branchId = $this->createBranch($clientWrapper, 'dev 123');
        $clientWrapper->setBranchId($branchId);

        $config = $this->getConfig();
        $expectedConfig = DestinationRewriter::rewriteDestination($config, $clientWrapper);
        self::assertEquals(sprintf('in.c-%s-main.table', $branchId), $expectedConfig['destination']);
        unset($expectedConfig['destination']);
        unset($config['destination']);
        self::assertEquals($config, $expectedConfig);
    }

    public function testRewriteNoBranch()
    {
        $clientWrapper = new ClientWrapper(
            new Client([
                'url' => STORAGE_API_URL,
                'token' => STORAGE_API_TOKEN_MASTER,
                'backoffMaxTries' => 1,
                'jobPollRetryDelay' => function () {
                    return 1;
                },
            ]),
            null,
            null
        );
        $clientWrapper->setBranchId('');
        $config = $this->getConfig();
        $expectedConfig = DestinationRewriter::rewriteDestination($config, $clientWrapper);
        self::assertEquals('in.c-main.table', $expectedConfig['destination']);
        unset($expectedConfig['destination']);
        unset($config['destination']);
        self::assertEquals($config, $expectedConfig);
    }

    public function testRewriteInvalidName()
    {
        $clientWrapper = new ClientWrapper(
            new Client([
                'url' => STORAGE_API_URL,
                'token' => STORAGE_API_TOKEN_MASTER,
                'backoffMaxTries' => 1,
                'jobPollRetryDelay' => function () {
                    return 1;
                },
            ]),
            null,
            null
        );
        $clientWrapper->setBranchId($this->createBranch($clientWrapper, 'dev-123'));
        $config = $this->getConfig();
        $config['destination'] = 'in.c-main-table';
        self::expectExceptionMessage('Invalid destination: "in.c-main-table"');
        self::expectException(InvalidOutputException::class);
        DestinationRewriter::rewriteDestination($config, $clientWrapper);
    }
}
