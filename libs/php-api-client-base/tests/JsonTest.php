<?php

declare(strict_types=1);

namespace Keboola\ApiClientBase\Tests;

use JsonException;
use Keboola\ApiClientBase\Json;
use PHPUnit\Framework\TestCase;

class JsonTest extends TestCase
{
    public function testEncodeArray(): void
    {
        self::assertSame('{"a":1}', Json::encodeArray(['a' => 1]));
    }

    public function testDecodeArray(): void
    {
        self::assertSame(['a' => 1], Json::decodeArray('{"a":1}'));
    }

    public function testDecodeInvalidJsonThrows(): void
    {
        $this->expectException(JsonException::class);
        Json::decodeArray('not-json');
    }

    public function testDecodeNonArrayThrows(): void
    {
        $this->expectException(JsonException::class);
        $this->expectExceptionMessage('Decoded data is int, array expected');
        Json::decodeArray('42');
    }
}
