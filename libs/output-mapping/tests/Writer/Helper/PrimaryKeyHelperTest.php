<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Helper;

use Keboola\Csv\CsvFile;
use Keboola\OutputMapping\Writer\Helper\PrimaryKeyHelper;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Exception;
use Keboola\Temp\Temp;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;
use Psr\Log\Test\TestLogger;

class PrimaryKeyHelperTest extends TestCase
{
    private const TEST_BUCKET_ID = 'out.c-PrimaryKeyHelperTest';
    private const TEST_TABLE_NAME = 'test-table';
    private const TEST_TABLE_ID = self::TEST_BUCKET_ID . '.' . self::TEST_TABLE_NAME;
    protected Client $client;

    public function setUp(): void
    {
        $this->client = new Client([
            'url' => (string) getenv('STORAGE_API_URL'),
            'token' => (string) getenv('STORAGE_API_TOKEN'),
        ]);
    }

    private function createTable(array $columns, string $primaryKey): void
    {
        if (!$this->client->bucketExists(self::TEST_BUCKET_ID)) {
            $this->client->createBucket('PrimaryKeyHelperTest', 'out');
        }
        try {
            $this->client->dropTable(self::TEST_TABLE_ID);
        } catch (Exception $e) {
            if ($e->getCode() !== 404) {
                throw $e;
            }
        }
        $temp = new Temp();
        $csv = new CsvFile($temp->getTmpFolder() . '/import.csv');
        $csv->writeRow($columns);
        $this->client->createTableAsync(
            self::TEST_BUCKET_ID,
            self::TEST_TABLE_NAME,
            $csv,
            ['primaryKey' => $primaryKey]
        );
    }

    /**
     * @dataProvider normalizePrimaryKeyProvider
     * @param array $pkey
     * @param array $result
     */
    public function testNormalizePrimaryKey(array $pkey, array $result): void
    {
        self::assertEquals($result, PrimaryKeyHelper::normalizeKeyArray(new NullLogger(), $pkey));
    }

    public function normalizePrimaryKeyProvider(): array
    {
        return [
            [
                [''],
                [],
            ],
            [
                ['Id', 'Id'],
                ['Id'],
            ],
            [
                ['Id ', 'Name'],
                ['Id', 'Name'],
            ],
        ];
    }

    /**
     * @dataProvider modifyPrimaryKeyDeciderOptionsProvider
     */
    public function testModifyPrimaryKeyDecider(
        array $currentTableInfo,
        array $newTableConfiguration,
        bool $result
    ): void {
        self::assertEquals($result, PrimaryKeyHelper::modifyPrimaryKeyDecider(
            new NullLogger(),
            $currentTableInfo,
            $newTableConfiguration
        ));
    }

    /**
     * @return array
     */
    public function modifyPrimaryKeyDeciderOptionsProvider(): array
    {
        return [
            [
                [
                    'primaryKey' => [],
                ],
                [
                    'primary_key' => [],
                ],
                false,
            ],
            [
                [
                    'primaryKey' => [],
                ],
                [
                    'primary_key' => ['Id'],
                ],
                true,
            ],
            [
                [
                    'primaryKey' => ['Id'],
                ],
                [
                    'primary_key' => [],
                ],
                true,
            ],
            [
                [
                    'primaryKey' => ['Id'],
                ],
                [
                    'primary_key' => ['Id'],
                ],
                false,
            ],
            [
                [
                    'primaryKey' => ['Id'],
                ],
                [
                    'primary_key' => ['Name'],
                ],
                true,
            ],
            [
                [
                    'primaryKey' => ['Id'],
                ],
                [
                    'primary_key' => ['Id', 'Name'],
                ],
                true,
            ],
        ];
    }

    public function testModifyPrimaryKeyChange(): void
    {
        $logger = new TestLogger();
        $this->createTable(['id', 'name', 'foo'], 'id,name');
        $tableInfo = $this->client->getTable(self::TEST_TABLE_ID);
        self::assertEquals(['id', 'name'], $tableInfo['primaryKey']);

        PrimaryKeyHelper::modifyPrimaryKey(
            $logger,
            $this->client,
            self::TEST_TABLE_ID,
            ['id', 'name'],
            ['id', 'foo']
        );
        self::assertTrue($logger->hasWarningThatContains(
            'Modifying primary key of table "' . self::TEST_BUCKET_ID . '.test-table" from "id, name" to "id, foo".'
        ));
        $tableInfo = $this->client->getTable(self::TEST_TABLE_ID);
        self::assertEquals(['id', 'foo'], $tableInfo['primaryKey']);
    }

    public function testModifyPrimaryKeyChangeFromEmpty(): void
    {
        $logger = new TestLogger();
        $this->createTable(['id', 'name', 'foo'], '');
        $tableInfo = $this->client->getTable(self::TEST_TABLE_ID);
        self::assertEquals([], $tableInfo['primaryKey']);

        PrimaryKeyHelper::modifyPrimaryKey(
            $logger,
            $this->client,
            self::TEST_TABLE_ID,
            [ ],
            ['id', 'foo']
        );
        self::assertTrue($logger->hasWarningThatContains(
            'Modifying primary key of table "' . self::TEST_BUCKET_ID . '.test-table" from "" to "id, foo".'
        ));
        $tableInfo = $this->client->getTable(self::TEST_TABLE_ID);
        self::assertEquals(['id', 'foo'], $tableInfo['primaryKey']);
    }

    public function testModifyPrimaryKeyErrorRemove(): void
    {
        $logger = new TestLogger();
        $this->createTable(['id', 'name', 'foo'], 'id,name');
        $tableInfo = $this->client->getTable(self::TEST_TABLE_ID);
        self::assertEquals(['id', 'name'], $tableInfo['primaryKey']);

        PrimaryKeyHelper::modifyPrimaryKey(
            $logger,
            $this->client,
            self::TEST_TABLE_ID . '-non-existent',
            ['id', 'name'],
            ['id', 'foo']
        );
        self::assertTrue($logger->hasWarningThatContains(
            'Modifying primary key of table "' . self::TEST_BUCKET_ID .
            '.test-table-non-existent" from "id, name" to "id, foo".'
        ));
        self::assertTrue($logger->hasWarningThatContains(
            'Error deleting primary key of table ' . self::TEST_BUCKET_ID . '.test-table-non-existent: The ' .
            'table "test-table-non-existent" was not found in the bucket "' . self::TEST_BUCKET_ID . '" in the project'
        ));
        $tableInfo = $this->client->getTable(self::TEST_TABLE_ID);
        self::assertEquals(['id', 'name'], $tableInfo['primaryKey']);
    }

    public function testModifyPrimaryKeyErrorCreate(): void
    {
        $logger = new TestLogger();
        $this->createTable(['id', 'name', 'foo'], 'id,name');
        $tableInfo = $this->client->getTable(self::TEST_TABLE_ID);
        self::assertEquals(['id', 'name'], $tableInfo['primaryKey']);

        PrimaryKeyHelper::modifyPrimaryKey(
            $logger,
            $this->client,
            self::TEST_TABLE_ID,
            ['id', 'name'],
            ['id', 'bar']
        );
        self::assertTrue($logger->hasWarningThatContains(
            'Modifying primary key of table "' . self::TEST_BUCKET_ID . '.test-table" from "id, name" to "id, bar".'
        ));
        self::assertTrue($logger->hasWarningThatContains(
            'Error changing primary key of table ' . self::TEST_BUCKET_ID . '.test-table: Primary key ' .
            'columns "bar" not found in "id, name, foo"'
        ));
        $tableInfo = $this->client->getTable(self::TEST_TABLE_ID);
        self::assertEquals(['id', 'name'], $tableInfo['primaryKey']);
    }
}
