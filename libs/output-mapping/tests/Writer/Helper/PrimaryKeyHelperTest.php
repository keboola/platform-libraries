<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Helper;

use Keboola\OutputMapping\Tests\AbstractTestCase;
use Keboola\OutputMapping\Writer\Helper\PrimaryKeyHelper;
use Psr\Log\NullLogger;

class PrimaryKeyHelperTest extends AbstractTestCase
{
    /**
     * @dataProvider normalizePrimaryKeyProvider
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
        bool $result,
    ): void {
        self::assertEquals($result, PrimaryKeyHelper::modifyPrimaryKeyDecider(
            new NullLogger(),
            $currentTableInfo['primaryKey'],
            $newTableConfiguration['primary_key'],
        ));
    }

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
}
