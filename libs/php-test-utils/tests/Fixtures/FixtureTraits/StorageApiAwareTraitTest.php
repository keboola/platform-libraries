<?php

declare(strict_types=1);

namespace Keboola\PhpTestUtils\Tests\Fixtures\FixtureTraits;

use Keboola\PhpTestUtils\Fixtures\FixtureTraits\StorageApiAwareTrait;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use PHPUnit\Framework\TestCase;

class StorageApiAwareTraitTest extends TestCase
{
    private function createObject(): object
    {
        return new class {
            use StorageApiAwareTrait;
        };
    }

    public function testCreateAndGetStorageClientWrapper(): void
    {
        $obj = $this->createObject();

        $hostnameSuffix = 'keboola.com';
        $token = 'dummy-token';

        self::assertTrue(method_exists($obj, 'createStorageClientWrapper'));
        self::assertTrue(method_exists($obj, 'getStorageClientWrapper'));

        $obj->createStorageClientWrapper($hostnameSuffix, $token);

        $wrapper = $obj->getStorageClientWrapper();

        self::assertInstanceOf(ClientWrapper::class, $wrapper);
        $options = $wrapper->getClientOptionsReadOnly();
        self::assertInstanceOf(ClientOptions::class, $options);
        self::assertSame($token, $options->getToken());
        self::assertSame('https://connection.keboola.com', $options->getUrl());
    }
}
