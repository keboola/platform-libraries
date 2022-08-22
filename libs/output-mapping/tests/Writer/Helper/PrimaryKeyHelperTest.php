<?php

namespace Keboola\OutputMapping\Tests\Writer\Helper;

use Keboola\Csv\CsvFile;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Writer\Helper\PrimaryKeyHelper;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Exception;
use Keboola\Temp\Temp;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;
use Psr\Log\Test\TestLogger;

class PrimaryKeyHelperTest extends TestCase
{
    const TEST_BUCKET_ID = 'out.c-' . self::class;
    const TEST_TABLE_NAME = 'test-table';
    const TEST_TABLE_ID = self::TEST_BUCKET_ID . '.' . self::TEST_TABLE_NAME;
    /**
     * @var Client
     */
    protected $client;

    public function setUp()
    {
        $this->client = new Client([
            'url' => STORAGE_API_URL,
            'token' => STORAGE_API_TOKEN,
        ]);
    }

    private function createTable(array $columns, $primaryKey)
    {
        if (!$this->client->bucketExists(self::TEST_BUCKET_ID)) {
            $this->client->createBucket(self::class, 'out');
        }
        try {
            $this->client->dropTable(self::TEST_TABLE_ID);
        } catch (Exception $e) {
            if ($e->getCode() !== 404) {
                throw $e;
            }
        }
        $temp = new Temp();
        $temp->initRunFolder();
        $csv = new CsvFile($temp->getTmpFolder() . '/import.csv');
        $csv->writeRow($columns);
        $this->client->createTableAsync(
            self::TEST_BUCKET_ID,
            self::TEST_TABLE_NAME,
            $csv,
            ['primaryKey' => $primaryKey]
        );
    }

    public function testValidateAgainstTable()
    {
        $tableInfo = ['primaryKey' => ['Id']];

        PrimaryKeyHelper::validatePrimaryKeyAgainstTable(
            new NullLogger(),
            $tableInfo,
            [
                'source' => 'table.csv',
                'destination' => self::TEST_BUCKET_ID . '.table',
                'primary_key' => ['Id'],
            ]
        );
        self::assertTrue(true);
    }

    public function testValidateAgainstTableEmptyPK()
    {
        $tableInfo = ['primaryKey' => []];

        PrimaryKeyHelper::validatePrimaryKeyAgainstTable(
            new NullLogger(),
            $tableInfo,
            [
                'source' => 'table.csv',
                'destination' => self::TEST_BUCKET_ID . '.table',
                'primary_key' => [],
            ]
        );
        self::assertTrue(true);
    }

    public function testValidateAgainstTableMismatch()
    {
        $tableInfo = ['primaryKey' => ['Id']];

        self::expectException(InvalidOutputException::class);
        self::expectExceptionMessage(
            'Output mapping does not match destination table: primary key "Id, Name" ' .
            'does not match "Id" in "' . self::TEST_BUCKET_ID . '.table".'
        );
        PrimaryKeyHelper::validatePrimaryKeyAgainstTable(
            new NullLogger(),
            $tableInfo,
            [
                'source' => 'table.csv',
                'destination' => self::TEST_BUCKET_ID . '.table',
                'primary_key' => ['Id', 'Name'],
            ]
        );
    }

    /**
     * @dataProvider normalizePrimaryKeyProvider
     * @param array $pkey
     * @param array $result
     */
    public function testNormalizePrimaryKey(array $pkey, array $result)
    {
        self::assertEquals($result, PrimaryKeyHelper::normalizeKeyArray(new NullLogger(), $pkey));
    }

    /**
     * @return array
     */
    public function normalizePrimaryKeyProvider()
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
     * @param array $tableInfo
     * @param array $config
     * @param $result
     */
    public function testModifyPrimaryKeyDecider(array $tableInfo, array $config, $result)
    {
        self::assertEquals($result, PrimaryKeyHelper::modifyPrimaryKeyDecider(new NullLogger(), $tableInfo, $config));
    }

    /**
     * @return array
     */
    public function modifyPrimaryKeyDeciderOptionsProvider()
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

    public function testModifyPrimaryKeyChange()
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
            'Modifying primary key of table "out.c-output-mapping-test.test-table" from "id, name" to "id, foo".'
        ));
        $tableInfo = $this->client->getTable(self::TEST_TABLE_ID);
        self::assertEquals(['id', 'foo'], $tableInfo['primaryKey']);
    }

    public function testModifyPrimaryKeyChangeFromEmpty()
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
            'Modifying primary key of table "out.c-output-mapping-test.test-table" from "" to "id, foo".'
        ));
        $tableInfo = $this->client->getTable(self::TEST_TABLE_ID);
        self::assertEquals(['id', 'foo'], $tableInfo['primaryKey']);
    }

    public function testModifyPrimaryKeyErrorRemove()
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
            'Modifying primary key of table "out.c-output-mapping-test.test-table-non-existent" from "id, name" to "id, foo".'
        ));
        self::assertTrue($logger->hasWarningThatContains(
            'Error deleting primary key of table out.c-output-mapping-test.test-table-non-existent: The ' .
            'table "test-table-non-existent" was not found in the bucket "out.c-output-mapping-test" in the project'
        ));
        $tableInfo = $this->client->getTable(self::TEST_TABLE_ID);
        self::assertEquals(['id', 'name'], $tableInfo['primaryKey']);
    }

    public function testModifyPrimaryKeyErrorCreate()
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
            'Modifying primary key of table "out.c-output-mapping-test.test-table" from "id, name" to "id, bar".'
        ));
        self::assertTrue($logger->hasWarningThatContains(
            'Error changing primary key of table out.c-output-mapping-test.test-table: Primary key ' .
            'columns "bar" not found in "id, name, foo"'
        ));
        $tableInfo = $this->client->getTable(self::TEST_TABLE_ID);
        self::assertEquals(['id', 'name'], $tableInfo['primaryKey']);
    }
}
