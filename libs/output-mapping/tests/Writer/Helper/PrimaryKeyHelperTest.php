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
}
