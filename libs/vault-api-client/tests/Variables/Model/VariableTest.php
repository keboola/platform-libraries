<?php

declare(strict_types=1);

namespace Keboola\VaultApiClient\Tests\Variables\Model;

use Keboola\VaultApiClient\Variables\Model\Variable;
use PHPUnit\Framework\TestCase;

class VariableTest extends TestCase
{
    public function testFromResponseData(): void
    {
        $data = [
            'hash' => 'hash',
            'key' => 'key',
            'value' => 'value',
            'isEncrypted' => true,
            'attributes' => [
                'attr1' => 'val1',
                'attr2' => 'val2',
            ],
        ];

        $variable = Variable::fromResponseData($data);
        self::assertEquals(new Variable(
            hash: 'hash',
            key: 'key',
            value: 'value',
            isEncrypted: true,
            attributes: [
                'attr1' => 'val1',
                'attr2' => 'val2',
            ],
        ), $variable);
    }
}
