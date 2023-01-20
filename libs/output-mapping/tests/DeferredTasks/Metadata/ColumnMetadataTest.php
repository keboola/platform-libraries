<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\DeferredTasks\Metadata;

use Keboola\OutputMapping\DeferredTasks\Metadata\ColumnMetadata;
use Keboola\StorageApi\Metadata;
use Keboola\StorageApi\Options\Metadata\TableMetadataUpdateOptions;
use PHPUnit\Framework\TestCase;

class ColumnMetadataTest extends TestCase
{
    public function testApply(): void
    {
        $metadataClientMock = $this->createMock(Metadata::class);
        $metadataClientMock->expects(self::once())
            ->method('postTableMetadataWithColumns')
            ->with(self::callback(function (TableMetadataUpdateOptions $options) {
                self::assertSame('in.c-testApply.table', $options->getTableId());
                self::assertSame(
                    [
                        'provider' => 'keboola.sample-component',
                        'columnsMetadata' => [
                            'id' => [
                                [
                                    'key' => 'timestamp',
                                    'value' => '1674226231',
                                ],
                            ],
                            'aa_caa' => [
                                [
                                    'key' => 'KBC.datatype.basetype',
                                    'value' => 'STRING',
                                ],
                                [
                                    'key' => '1',
                                    'value' => '',
                                ],
                            ],
                        ],
                    ],
                    $options->toParamsArray()
                );
                return true;
            }))
        ;

        $columnMetadata = new ColumnMetadata(
            'in.c-testApply.table',
            'keboola.sample-component',
            [
                'id' => [
                    [
                        'key' => 'timestamp',
                        'value' => 1674226231,
                    ],
                ],
                'aa/čaa_' => [ // column name will be sanitized by ColumnNameSanitizer
                    [
                        'key' => 'KBC.datatype.basetype',
                        'value' => 'STRING',
                    ],
                    [
                        'key' => 1,
                        'value' => '',
                    ],
                ],
            ]
        );

        $columnMetadata->apply($metadataClientMock);
    }
}
