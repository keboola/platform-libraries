<?php

declare(strict_types=1);

namespace Keboola\VaultApiClient\Tests\Variables\Model;

use Keboola\VaultApiClient\Variables\Model\ListOptions;
use PHPUnit\Framework\TestCase;

class ListOptionsTest extends TestCase
{
    public function testEmptyOptionsAsQueryString(): void
    {
        $options = new ListOptions();
        self::assertSame('', $options->asQueryString());
    }

    public function testAllOptionsAsQueryString(): void
    {
        $options = new ListOptions(
            key: 'key',
            attributes: ['attr1' => 'val1', 'attr2' => 'val2'],
            offset: 10,
            limit: 20,
        );
        self::assertSame(
            'key=key&attributes%5Battr1%5D=val1&attributes%5Battr2%5D=val2&offset=10&limit=20',
            $options->asQueryString(),
        );
    }
}
