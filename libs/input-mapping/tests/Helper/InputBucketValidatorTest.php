<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Helper;

use Keboola\InputMapping\Exception\InvalidInputException;
use Keboola\InputMapping\Helper\InputBucketValidator;
use Keboola\InputMapping\Table\Options\InputTableOptionsList;
use Keboola\InputMapping\Tests\AbstractTestCase;
use Keboola\InputMapping\Tests\Needs\NeedsEmptyInputBucket;
use Keboola\InputMapping\Tests\Needs\NeedsEmptyOutputBucket;
use Keboola\StorageApi\Metadata;

class InputBucketValidatorTest extends AbstractTestCase
{
    private function initBuckets(bool $hasMetadata): void
    {
        if ($hasMetadata) {
            $metadataApi = new Metadata($this->clientWrapper->getTableAndFileStorageClient());
            $metadataApi->postBucketMetadata(
                $this->emptyOutputBucketId,
                'test',
                [
                    [
                        'key' => 'KBC.createdBy.branch.id',
                        'value' => '1234',
                    ],
                ],
            );
            $metadataApi->postBucketMetadata(
                $this->emptyInputBucketId,
                'test',
                [
                    [
                        'key' => 'KBC.lastUpdatedBy.branch.id',
                        'value' => '1235',
                    ],
                ],
            );
        }
    }

    #[NeedsEmptyInputBucket, NeedsEmptyOutputBucket]
    public function testClean(): void
    {
        $this->initBuckets(false);
        $inputTablesOptions = new InputTableOptionsList([
            [
                'source' => $this->emptyInputBucketId . '.my-table',
                'destination' => 'my-table.csv',
                'days' => 12,
                'columns' => ['id', 'name'],
            ],
            [
                'source' => $this->emptyOutputBucketId . '.my-table-2',
                'destination' => 'my-table-2.csv',
                'columns' => ['foo', 'bar'],
            ],
        ]);
        InputBucketValidator::checkDevBuckets($inputTablesOptions, $this->clientWrapper);
        self::assertTrue(true);
    }

    #[NeedsEmptyInputBucket, NeedsEmptyOutputBucket]
    public function testTainted(): void
    {
        $this->initBuckets(true);
        $inputTablesOptions = new InputTableOptionsList([
            [
                'source' => $this->emptyInputBucketId . '.my-table',
                'destination' => 'my-table.csv',
                'days' => 12,
                'columns' => ['id', 'name'],
            ],
            [
                'source' => $this->emptyOutputBucketId . '.my-table-2',
                'destination' => 'my-table-2.csv',
                'columns' => ['foo', 'bar'],
            ],
        ]);
        $this->expectException(InvalidInputException::class);
        $this->expectExceptionMessage(sprintf(
            'The buckets "%s, %s" come from a development branch and must not be used directly in input mapping.',
            $this->emptyInputBucketId,
            $this->emptyOutputBucketId,
        ));
        InputBucketValidator::checkDevBuckets($inputTablesOptions, $this->clientWrapper);
    }

    public function testNonExistent(): void
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
