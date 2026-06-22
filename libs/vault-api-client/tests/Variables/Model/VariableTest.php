<?php

declare(strict_types=1);

namespace Keboola\VaultApiClient\Tests\Variables\Model;

use InvalidArgumentException;
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
            'flags' => ['encrypted'],
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
            flags: [Variable::FLAG_ENCRYPTED],
            attributes: [
                'attr1' => 'val1',
                'attr2' => 'val2',
            ],
        ), $variable);
    }

    public function testFromResponseDataWithDefaultFlagsAndAttributes(): void
    {
        $data = [
            'hash' => 'hash',
            'key' => 'key',
            'value' => 'value',
        ];

        $variable = Variable::fromResponseData($data);
        self::assertEquals([], $variable->flags);
        self::assertEquals([], $variable->attributes);
    }

    public function testFromResponseDataThrowsOnEmptyHash(): void
    {
        $this->expectException(InvalidArgumentException::class);

        Variable::fromResponseData([
            'hash' => '',
            'key' => 'key',
            'value' => 'value',
        ]);
    }

    public function testFromResponseDataThrowsOnEmptyKey(): void
    {
        $this->expectException(InvalidArgumentException::class);

        Variable::fromResponseData([
            'hash' => 'hash',
            'key' => '',
            'value' => 'value',
        ]);
    }

    public function testFromResponseDataThrowsOnNonStringValue(): void
    {
        $this->expectException(InvalidArgumentException::class);

        Variable::fromResponseData([
            'hash' => 'hash',
            'key' => 'key',
            'value' => 123,
        ]);
    }

    public function testFromResponseDataThrowsOnNonStringFlags(): void
    {
        $this->expectException(InvalidArgumentException::class);

        Variable::fromResponseData([
            'hash' => 'hash',
            'key' => 'key',
            'value' => 'value',
            'flags' => [123],
        ]);
    }

    public function testFromResponseDataThrowsOnNonStringAttributes(): void
    {
        $this->expectException(InvalidArgumentException::class);

        Variable::fromResponseData([
            'hash' => 'hash',
            'key' => 'key',
            'value' => 'value',
            'attributes' => ['key' => 123],
        ]);
    }

    public function testFromResponseDataThrowsOnNonArrayFlags(): void
    {
        $this->expectException(InvalidArgumentException::class);

        Variable::fromResponseData([
            'hash' => 'hash',
            'key' => 'key',
            'value' => 'value',
            'flags' => 123,
        ]);
    }

    public function testFromResponseDataThrowsOnIndexedAttributes(): void
    {
        $this->expectException(InvalidArgumentException::class);

        Variable::fromResponseData([
            'hash' => 'hash',
            'key' => 'key',
            'value' => 'value',
            'attributes' => ['val1', 'val2'],
        ]);
    }
}
