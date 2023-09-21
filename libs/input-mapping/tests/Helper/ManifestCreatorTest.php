<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Helper;

use Generator;
use Keboola\InputMapping\Configuration\Adapter;
use Keboola\InputMapping\Helper\ManifestCreator;
use Keboola\Temp\Temp;
use PHPUnit\Framework\TestCase;

class ManifestCreatorTest extends TestCase
{
    public function createFileManifestData(): Generator
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

    public function writeTableManifestData(): Generator
    {
        yield 'json format' => [
            'format' => 'json',
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
    }
}
EOF,
        ];
        yield 'json format with columns override' => [
            'format' => 'json',
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
    }
}
EOF,
        ];
        yield 'yaml format' => [
            'format' => 'yaml',
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

EOF,
        ];
        yield 'yaml format with columns override' => [
            'format' => 'yaml',
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

EOF,
        ];
    }

    /**
     * @dataProvider writeTableManifestData
     * @param Adapter::FORMAT_YAML | Adapter::FORMAT_JSON $format
     */
    public function testWriteTableManifest(
        string $format,
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
