<?php

declare(strict_types=1);

namespace Keboola\VaultApiClient\Tests;

use JsonException;
use Keboola\VaultApiClient\Json;
use PHPUnit\Framework\TestCase;

class JsonTest extends TestCase
{
    public function testEncodeArray(): void
    {
        self::assertSame(
            '{"foo":"bar"}',
            Json::encodeArray(['foo' => 'bar']),
        );
    }

    public function testDecodeArray(): void
    {
        self::assertSame(
            ['foo' => 'bar'],
            Json::decodeArray('{"foo":"bar"}'),
        );
    }

    /** @dataProvider provideDecodeArrayTestData */
    public function testDecodeArrayError(string $data, string $expectedError): void
    {
        $this->expectException(JsonException::class);
        $this->expectExceptionMessage($expectedError);

        Json::decodeArray($data);
    }

    public function provideDecodeArrayTestData(): iterable
    {
        yield 'invalid JSON' => [
            'data' => '{"foo"',
            'error' => 'Syntax error',
        ];

        yield 'not an array' => [
            'data' => '"foo"',
            'error' => 'Decoded data is string, array expected',
        ];
    }
}
