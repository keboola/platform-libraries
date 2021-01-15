<?php

namespace Keboola\InputMapping\Tests\Helper;

use Keboola\InputMapping\Exception\InvalidInputException;
use Keboola\InputMapping\Helper\InputBucketValidator;
use Keboola\InputMapping\Table\Options\InputTableOptionsList;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\DevBranches;
use Keboola\StorageApi\Metadata;
use Keboola\StorageApiBranch\ClientWrapper;
use PHPUnit\Framework\TestCase;
use Psr\Log\Test\TestLogger;

class InputBucketValidatorTest extends TestCase
{
    /** @var ClientWrapper */
    private $clientWrapper;

    public function setUp()
    {
        parent::setUp();
        $this->clientWrapper = new ClientWrapper(
            new Client(['token' => STORAGE_API_TOKEN_MASTER, "url" => STORAGE_API_URL]),
            null,
            null
        );
        $this->clientWrapper->setBranchId('');
    }

    private function initBuckets($hasMetadata)
    {
        $buckets = ['out.c-input-mapping-validator', 'in.c-input-mapping-validator'];
        foreach ($buckets as $bucket) {
            try {
                $this->clientWrapper->getBasicClient()->dropBucket($bucket, ['force' => true]);
            } catch (ClientException $e) {
                if ($e->getCode() !== 404) {
                    throw $e;
                }
            }
        }

        $this->clientWrapper->getBasicClient()->createBucket('input-mapping-validator', 'in');
        $this->clientWrapper->getBasicClient()->createBucket('input-mapping-validator', 'out');
        if ($hasMetadata) {
            $hasMetadata = new Metadata($this->clientWrapper->getBasicClient());
            $hasMetadata->postBucketMetadata(
                'out.c-input-mapping-validator',
                'test',
                [
                    [
                        'key' => 'KBC.createdBy.branch.id',
                        'value' => '1234',
                    ],
                ]
            );
            $hasMetadata->postBucketMetadata(
                'in.c-input-mapping-validator',
                'test',
                [
                    [
                        'key' => 'KBC.lastUpdatedBy.branch.id',
                        'value' => '1235',
                    ],
                ]
            );
        }
    }

    public function testClean()
    {
        $this->initBuckets(false);
        $inputTablesOptions = new InputTableOptionsList([
            [
                'source' => 'in.c-input-mapping-validator.my-table',
                'destination' => 'my-table.csv',
                'days' => 12,
                'columns' => ['id', 'name'],
            ],
            [
                'source' => 'out.c-input-mapping-validator.my-table-2',
                'destination' => 'my-table-2.csv',
                'columns' => ['foo', 'bar'],
            ],
        ]);
        InputBucketValidator::checkDevBuckets($inputTablesOptions, $this->clientWrapper);
        self::assertTrue(true);
    }

    public function testTainted()
    {
        $this->initBuckets(true);
        $inputTablesOptions = new InputTableOptionsList([
            [
                'source' => 'in.c-input-mapping-validator.my-table',
                'destination' => 'my-table.csv',
                'days' => 12,
                'columns' => ['id', 'name'],
            ],
            [
                'source' => 'out.c-input-mapping-validator.my-table-2',
                'destination' => 'my-table-2.csv',
                'columns' => ['foo', 'bar'],
            ],
        ]);
        self::expectException(InvalidInputException::class);
        self::expectExceptionMessage(
            'The buckets "in.c-input-mapping-validator, out.c-input-mapping-validator" ' .
            'come from a development branch and must not be used directly in input mapping.'
        );
        InputBucketValidator::checkDevBuckets($inputTablesOptions, $this->clientWrapper);
    }

    public function testNonExistent()
    {
        $inputTablesOptions = new InputTableOptionsList([
            [
                'source' => 'out.c-non-existent.my-table',
                'destination' => 'my-table.csv',
                'days' => 12,
                'columns' => ['id', 'name'],
            ],
            [
                'source' => 'in.c-non-existent.my-table-2',
                'destination' => 'my-table-2.csv',
                'columns' => ['foo', 'bar'],
            ],
        ]);
        InputBucketValidator::checkDevBuckets($inputTablesOptions, $this->clientWrapper);
        self::assertTrue(true);
    }
}
