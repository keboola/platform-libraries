<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Helper;

use Keboola\InputMapping\Helper\ManifestCreator;
use Keboola\StagingProvider\Staging\File\FileFormat;
use Keboola\Temp\Temp;
use PHPUnit\Framework\TestCase;

class ManifestCreatorTest extends TestCase
{
    public function createFileManifestData(): iterable
    {
        yield 'only isPublic' => [
            'isPublic' => true,
            'isSliced' => false,
            'isEncrypted' => false,
        ];
        yield 'only isSliced' => [
            'isPublic' => false,
            'isSliced' => true,
            'isEncrypted' => false,
        ];
        yield 'only isEncrypted' => [
            'isPublic' => false,
            'isSliced' => false,
            'isEncrypted' => true,
        ];
    }

    /**
     * @dataProvider createFileManifestData
     */
    public function testCreateFileManifest(
        bool $expectedIsPublic,
        bool $expectedIsSliced,
        bool $expectedIsEncrypted,
    ): void {
        $fileInfo = [
            'id' => 18311387,
            'name' => 'testCreateFileManifest.txt',
            'created' => '2022-05-28T10:31:13+0200',
            'isPublic' => $expectedIsPublic,
            'isSliced' => $expectedIsSliced,
            'isEncrypted' => $expectedIsEncrypted,
            'tags' => ['tag1', 'tag2'],
            'sizeBytes' => 1024,
            'maxAgeDays' => 15,
        ];

        $manifestCreator = new ManifestCreator();

        $manifest = $manifestCreator->createFileManifest($fileInfo);

        self::assertSame([
            'id',
            'name',
            'created',
            'is_public',
            'is_encrypted',
            'is_sliced',
            'tags',
            'max_age_days',
            'size_bytes',
        ], array_keys($manifest));

        self::assertSame($fileInfo['id'], $manifest['id']);
        self::assertSame($fileInfo['name'], $manifest['name']);
        self::assertSame($fileInfo['created'], $manifest['created']);
        self::assertSame($fileInfo['isPublic'], $manifest['is_public']);
        self::assertSame($fileInfo['isEncrypted'], $manifest['is_encrypted']);
        self::assertSame($fileInfo['isSliced'], $manifest['is_sliced']);
        self::assertSame($fileInfo['tags'], $manifest['tags']);
        self::assertSame($fileInfo['maxAgeDays'], $manifest['max_age_days']);
        self::assertSame($fileInfo['sizeBytes'], $manifest['size_bytes']);
    }

    public function writeTableManifestData(): iterable
    {
        yield 'json format' => [
            'format' => FileFormat::Json,
            'columns' => [],
            'expectedData' => <<<'EOF'
{
    "id": "in.c-docker-test.test",
    "uri": "https:\/\/connection.keboola.com\/v2\/storage\/tables\/in.c-docker-test.test",
    "name": "test",
    "primary_key": [
        "Id"
    ],
    "distribution_key": [
        "foo"
    ],
    "created": "2022-06-03T01:31:43+0200",
    "last_change_date": "2022-06-03T02:31:43+0200",
    "last_import_date": "2022-06-03T03:31:43+0200",
    "columns": [
        "Id",
        "Name",
        "foo",
        "bar"
    ],
    "metadata": [
        {
            "id": "123",
            "key": "description",
            "value": "Test",
            "provider": "input-mapping",
            "timestamp": "2022-06-03T04:31:43+0200"
        }
    ],
    "column_metadata": {
        "Id": [
            {
                "id": "456",
                "key": "datatype",
                "value": "NUMBER",
                "provider": "input-mapping",
                "timestamp": "2022-06-03T05:31:43+0200"
            }
        ],
        "Name": [
            {
                "id": "789",
                "key": "datatype",
                "value": "TEXT",
                "provider": "input-mapping",
                "timestamp": "2022-06-03T06:31:43+0200"
            }
        ],
        "foo": [],
        "bar": []
    },
    "schema": []
}
EOF,
        ];
        yield 'json format with columns override' => [
            'format' => FileFormat::Json,
            'columns' => ['Name'],
            'expectedData' => <<<'EOF'
{
    "id": "in.c-docker-test.test",
    "uri": "https:\/\/connection.keboola.com\/v2\/storage\/tables\/in.c-docker-test.test",
    "name": "test",
    "primary_key": [
        "Id"
    ],
    "distribution_key": [
        "foo"
    ],
    "created": "2022-06-03T01:31:43+0200",
    "last_change_date": "2022-06-03T02:31:43+0200",
    "last_import_date": "2022-06-03T03:31:43+0200",
    "columns": [
        "Name"
    ],
    "metadata": [
        {
            "id": "123",
            "key": "description",
            "value": "Test",
            "provider": "input-mapping",
            "timestamp": "2022-06-03T04:31:43+0200"
        }
    ],
    "column_metadata": {
        "Name": [
            {
                "id": "789",
                "key": "datatype",
                "value": "TEXT",
                "provider": "input-mapping",
                "timestamp": "2022-06-03T06:31:43+0200"
            }
        ]
    },
    "schema": []
}
EOF,
        ];
        yield 'yaml format' => [
            'format' => FileFormat::Yaml,
            'columns' => [],
            'expectedData' => <<<'EOF'
id: in.c-docker-test.test
uri: 'https://connection.keboola.com/v2/storage/tables/in.c-docker-test.test'
name: test
primary_key:
    - Id
distribution_key:
    - foo
created: '2022-06-03T01:31:43+0200'
last_change_date: '2022-06-03T02:31:43+0200'
last_import_date: '2022-06-03T03:31:43+0200'
columns:
    - Id
    - Name
    - foo
    - bar
metadata:
    -
        id: '123'
        key: description
        value: Test
        provider: input-mapping
        timestamp: '2022-06-03T04:31:43+0200'
column_metadata:
    Id:
        -
            id: '456'
            key: datatype
            value: NUMBER
            provider: input-mapping
            timestamp: '2022-06-03T05:31:43+0200'
    Name:
        -
            id: '789'
            key: datatype
            value: TEXT
            provider: input-mapping
            timestamp: '2022-06-03T06:31:43+0200'
    foo: {  }
    bar: {  }
schema: {  }

EOF,
        ];
        yield 'yaml format with columns override' => [
            'format' => FileFormat::Yaml,
            'columns' => ['Name'],
            'expectedData' => <<<'EOF'
id: in.c-docker-test.test
uri: 'https://connection.keboola.com/v2/storage/tables/in.c-docker-test.test'
name: test
primary_key:
    - Id
distribution_key:
    - foo
created: '2022-06-03T01:31:43+0200'
last_change_date: '2022-06-03T02:31:43+0200'
last_import_date: '2022-06-03T03:31:43+0200'
columns:
    - Name
metadata:
    -
        id: '123'
        key: description
        value: Test
        provider: input-mapping
        timestamp: '2022-06-03T04:31:43+0200'
column_metadata:
    Name:
        -
            id: '789'
            key: datatype
            value: TEXT
            provider: input-mapping
            timestamp: '2022-06-03T06:31:43+0200'
schema: {  }

EOF,
        ];
    }

    public function testWriteTableManifestBuildsSchemaFromDefinition(): void
    {
        $temp = new Temp('docker');
        $filePathname = (string) $temp->createTmpFile();

        $tableInfo = $this->getTableInfoWithDefinition();

        $manifestCreator = new ManifestCreator();
        $manifestCreator->writeTableManifest($tableInfo, $filePathname, [], FileFormat::Json);

        $manifest = (array) json_decode((string) file_get_contents($filePathname), true);

        // table description comes from the native definition field
        self::assertSame('native table description', $manifest['description']);

        // schema is built from the native definition: data types reflect the table's backend, descriptions and
        // defaults are native-only, columns without a native description simply omit the description key
        self::assertEquals(
            [
                [
                    'name' => 'id',
                    'data_type' => [
                        'base' => ['type' => 'INTEGER'],
                        'snowflake' => ['type' => 'NUMBER', 'length' => '38,0'],
                    ],
                    'nullable' => false,
                    'primary_key' => true,
                    'description' => 'native id description',
                ],
                [
                    'name' => 'name',
                    'data_type' => [
                        'base' => ['type' => 'STRING'],
                        'snowflake' => ['type' => 'VARCHAR', 'length' => '16777216'],
                    ],
                    'nullable' => true,
                    'primary_key' => false,
                ],
                [
                    'name' => 'size',
                    'data_type' => [
                        'base' => ['type' => 'INTEGER'],
                        'snowflake' => ['type' => 'NUMBER', 'length' => '38,0'],
                    ],
                    'nullable' => true,
                    'primary_key' => false,
                    'description' => 'native size description',
                ],
                [
                    'name' => 'flag',
                    'data_type' => [
                        'base' => ['type' => 'INTEGER'],
                        'snowflake' => ['type' => 'INT', 'default' => '12'],
                    ],
                    'nullable' => true,
                    'primary_key' => false,
                ],
            ],
            $manifest['schema'],
        );

        // legacy metadata structures are left completely untouched (Connection backfills KBC.description there)
        self::assertEquals($tableInfo['metadata'], $manifest['metadata']);
        self::assertEquals($tableInfo['columnMetadata']['id'], $manifest['column_metadata']['id']);
    }

    public function testWriteTableManifestEmptySchemaWhenNoDefinition(): void
    {
        $temp = new Temp('docker');
        $filePathname = (string) $temp->createTmpFile();

        $tableInfo = $this->getTableInfoWithDefinition();
        unset($tableInfo['definition']);

        $manifestCreator = new ManifestCreator();
        $manifestCreator->writeTableManifest($tableInfo, $filePathname, [], FileFormat::Json);

        $manifest = (array) json_decode((string) file_get_contents($filePathname), true);

        self::assertSame([], $manifest['schema']);
        self::assertArrayNotHasKey('description', $manifest);
        self::assertEquals($tableInfo['metadata'], $manifest['metadata']);
    }

    public function testWriteTableManifestEmptyTableDescriptionIsOmitted(): void
    {
        $temp = new Temp('docker');
        $filePathname = (string) $temp->createTmpFile();

        $tableInfo = $this->getTableInfoWithDefinition();
        $tableInfo['definition']['description'] = '';
        $tableInfo['definition']['columns'][0]['definition']['description'] = '';

        $manifestCreator = new ManifestCreator();
        $manifestCreator->writeTableManifest($tableInfo, $filePathname, [], FileFormat::Json);

        $manifest = (array) json_decode((string) file_get_contents($filePathname), true);
        $schema = $manifest['schema'];
        self::assertIsArray($schema);
        self::assertIsArray($schema[0]);
        self::assertIsArray($schema[2]);

        // empty native description is treated as absent
        self::assertArrayNotHasKey('description', $manifest);
        self::assertArrayNotHasKey('description', $schema[0]);
        // a sibling column keeps its non-empty native description
        self::assertSame('native size description', $schema[2]['description']);
    }

    public function testWriteTableManifestSchemaRespectsColumnSelection(): void
    {
        $temp = new Temp('docker');
        $filePathname = (string) $temp->createTmpFile();

        $tableInfo = $this->getTableInfoWithDefinition();

        $manifestCreator = new ManifestCreator();
        $manifestCreator->writeTableManifest($tableInfo, $filePathname, ['name'], FileFormat::Json);

        $manifest = (array) json_decode((string) file_get_contents($filePathname), true);
        $schema = $manifest['schema'];
        self::assertIsArray($schema);
        self::assertIsArray($schema[0]);

        // schema only contains the selected columns, in the selected order
        self::assertCount(1, $schema);
        self::assertSame('name', $schema[0]['name']);
    }

    public function testWriteTableManifestBuildsSchemaForNonTypedTable(): void
    {
        $temp = new Temp('docker');
        $filePathname = (string) $temp->createTmpFile();

        // a non-typed table still has a definition with column names and primary keys, but the column definitions
        // carry no data types - only an optional native description
        $tableInfo = [
            'uri' => 'https://connection.keboola.com/v2/storage/tables/in.c-docker-test.test',
            'id' => 'in.c-docker-test.test',
            'name' => 'test',
            'primaryKey' => ['Id'],
            'distributionKey' => [],
            'created' => '2022-06-03T01:31:43+0200',
            'lastChangeDate' => '2022-06-03T02:31:43+0200',
            'lastImportDate' => '2022-06-03T03:31:43+0200',
            'columns' => ['Id', 'Name'],
            'bucket' => ['id' => 'in.c-docker-test', 'backend' => 'snowflake'],
            'metadata' => [],
            'columnMetadata' => ['Id' => [], 'Name' => []],
            'definition' => [
                'primaryKeysNames' => ['Id'],
                'columns' => [
                    ['name' => 'Id', 'definition' => ['description' => 'native id description']],
                    ['name' => 'Name', 'definition' => []],
                ],
            ],
        ];

        $manifestCreator = new ManifestCreator();
        $manifestCreator->writeTableManifest($tableInfo, $filePathname, [], FileFormat::Json);

        $manifest = (array) json_decode((string) file_get_contents($filePathname), true);

        // schema is emitted with no data types; native description still propagates, primary key still resolved
        self::assertEquals(
            [
                [
                    'name' => 'Id',
                    'primary_key' => true,
                    'description' => 'native id description',
                ],
                [
                    'name' => 'Name',
                    'primary_key' => false,
                ],
            ],
            $manifest['schema'],
        );
        // no native table description -> top-level description omitted
        self::assertArrayNotHasKey('description', $manifest);
    }

    private function getTableInfoWithDefinition(): array
    {
        return [
            'uri' => 'https://connection.keboola.com/v2/storage/tables/in.c-docker-test.test',
            'id' => 'in.c-docker-test.test',
            'name' => 'test',
            'primaryKey' => ['id'],
            'distributionKey' => [],
            'created' => '2022-06-03T01:31:43+0200',
            'lastChangeDate' => '2022-06-03T02:31:43+0200',
            'lastImportDate' => '2022-06-03T03:31:43+0200',
            'columns' => ['id', 'name', 'size', 'flag'],
            'bucket' => [
                'id' => 'in.c-docker-test',
                'backend' => 'snowflake',
            ],
            'metadata' => [
                [
                    'id' => '123',
                    'key' => 'KBC.description',
                    'value' => 'stale metadata description',
                    'provider' => 'storage',
                    'timestamp' => '2022-06-03T04:31:43+0200',
                ],
            ],
            'columnMetadata' => [
                'id' => [
                    [
                        'id' => '456',
                        'key' => 'KBC.description',
                        'value' => 'stale id metadata description',
                        'provider' => 'storage',
                        'timestamp' => '2022-06-03T05:31:43+0200',
                    ],
                ],
                'name' => [],
                'size' => [],
                'flag' => [],
            ],
            'definition' => [
                'primaryKeysNames' => ['id'],
                'columns' => [
                    [
                        'name' => 'id',
                        'definition' => [
                            'type' => 'NUMBER',
                            'nullable' => false,
                            'length' => '38,0',
                            'description' => 'native id description',
                        ],
                        'basetype' => 'INTEGER',
                    ],
                    [
                        'name' => 'name',
                        'definition' => [
                            'type' => 'VARCHAR',
                            'nullable' => true,
                            'length' => '16777216',
                        ],
                        'basetype' => 'STRING',
                    ],
                    [
                        'name' => 'size',
                        'definition' => [
                            'type' => 'NUMBER',
                            'nullable' => true,
                            'length' => '38,0',
                            'description' => 'native size description',
                        ],
                        'basetype' => 'INTEGER',
                    ],
                    [
                        'name' => 'flag',
                        'definition' => [
                            'type' => 'INT',
                            'nullable' => true,
                            'default' => '12',
                        ],
                        'basetype' => 'INTEGER',
                    ],
                ],
                'description' => 'native table description',
            ],
        ];
    }

    /**
     * @dataProvider writeTableManifestData
     */
    public function testWriteTableManifest(
        FileFormat $format,
        array $columns,
        string $expectedData,
    ): void {
        $temp = new Temp('docker');
        $filePathname = (string) $temp->createTmpFile();

        $tableInfo = [
            'uri' => 'https://connection.keboola.com/v2/storage/tables/in.c-docker-test.test',
            'id' => 'in.c-docker-test.test',
            'name' => 'test',
            'primaryKey' => ['Id'],
            'distributionKey' => ['foo'],
            'created' => '2022-06-03T01:31:43+0200',
            'lastChangeDate' => '2022-06-03T02:31:43+0200',
            'lastImportDate' => '2022-06-03T03:31:43+0200',
            'columns' => ['Id', 'Name', 'foo', 'bar'],
            'metadata' => [
                [
                    'id' => '123',
                    'key' => 'description',
                    'value' => 'Test',
                    'provider' => 'input-mapping',
                    'timestamp' => '2022-06-03T04:31:43+0200',
                ],
            ],
            'columnMetadata' => [
                'Id' => [
                    [
                        'id' => '456',
                        'key' => 'datatype',
                        'value' => 'NUMBER',
                        'provider' => 'input-mapping',
                        'timestamp' => '2022-06-03T05:31:43+0200',
                    ],
                ],
                'Name' => [
                    [
                        'id' => '789',
                        'key' => 'datatype',
                        'value' => 'TEXT',
                        'provider' => 'input-mapping',
                        'timestamp' => '2022-06-03T06:31:43+0200',
                    ],
                ],
                'foo' => [],
                'bar' => [],
            ],
        ];

        $manifestCreator = new ManifestCreator();
        $manifestCreator->writeTableManifest($tableInfo, $filePathname, $columns, $format);

        self::assertSame($expectedData, file_get_contents($filePathname));
    }
}
