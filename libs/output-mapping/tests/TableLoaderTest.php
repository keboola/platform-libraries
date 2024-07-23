<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests;

use Generator;
use Keboola\InputMapping\Staging\AbstractStrategyFactory;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\OutputMappingSettings;
use Keboola\OutputMapping\SystemMetadata;
use Keboola\OutputMapping\TableLoader;
use Keboola\OutputMapping\Tests\Needs\NeedsEmptyInputBucket;
use Keboola\OutputMapping\Writer\AbstractWriter;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApiBranch\StorageApiToken;

class TableLoaderTest extends AbstractTestCase
{
    public function uploadTablesHandlesStorageErrorsTableCreateProvider(): Generator
    {
        yield 'non-typed table' => [
            'manifestData' => [
                'destination' => 'testTable',
                'columns' => [
                    'id',
                    'name',
                ],
                'primary_key' => ['nonExistentColumn'],
            ],
            'hasNewNativeTypeFeature' => false,
            'expectedErrorCode' => 'storage.tables.validation.invalidPrimaryKeyColumns',
        ];

        yield 'typed table' => [
            'manifestData' => [
                'destination' => 'testTable',
                'schema' => [
                    [
                        'name' => str_repeat('a', 100),
                        'data_type' => [
                            'base' => [
                                'type' => 'STRING',
                            ],
                        ],
                    ],
                ],
            ],
            'hasNewNativeTypeFeature' => true,
            'expectedErrorCode' => 'validation.failed',
        ];
    }

    /**
     * @dataProvider uploadTablesHandlesStorageErrorsTableCreateProvider
     */
    #[NeedsEmptyInputBucket]
    public function testUploadTablesHandlesStorageErrorsTableCreate(
        array $manifestData,
        bool $hasNewNativeTypeFeature,
        string $expectedErrorCode,
    ): void {
        $stagingFactory = $this->getLocalStagingFactory();

        touch($this->temp->getTmpFolder(). '/upload/source1');
        file_put_contents($this->temp->getTmpFolder(). '/upload/source1.manifest', json_encode($manifestData));

        $tableLoader = new TableLoader(
            $this->testLogger,
            $this->clientWrapper,
            $stagingFactory,
        );

        $token = $this->createMock(StorageApiToken::class);
        $token->method('hasFeature')
            ->willReturnCallback(function (string $feature) use ($hasNewNativeTypeFeature): bool {
                if ($feature === OutputMappingSettings::NEW_NATIVE_TYPES_FEATURE) {
                    return $hasNewNativeTypeFeature;
                }
                return false;
            })
        ;

        $configuration = new OutputMappingSettings(
            [
                'bucket' => $this->emptyInputBucketId,
            ],
            'upload',
            $token,
            false,
            OutputMappingSettings::DATA_TYPES_SUPPORT_AUTHORITATIVE,
        );

        try {
            $tableLoader->uploadTables(
                AbstractStrategyFactory::LOCAL,
                $configuration,
                new SystemMetadata([
                    AbstractWriter::SYSTEM_KEY_COMPONENT_ID => 'keboola.output-mapping',
                ]),
            );
            $this->fail('UploadTables should fail with InvalidOutputException');
        } catch (InvalidOutputException $e) {
            self::assertStringContainsString(
                'Cannot create table "testTable"',
                $e->getMessage(),
            );
            self::assertStringContainsString($expectedErrorCode, $e->getMessage());
            self::assertNotNull($e->getPrevious());
            self::assertInstanceOf(ClientException::class, $e->getPrevious());
        }
    }
}
