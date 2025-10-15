<?php

declare(strict_types=1);

namespace Keboola\PhpTestUtils\Tests\Fixtures\FixtureTraits;

use Keboola\PhpTestUtils\Fixtures\FixtureTraits\StorageTokenTrait;
use PHPUnit\Framework\TestCase;

class StorageTokenTraitTest extends TestCase
{
    private function createObject(): object
    {
        return new class {
            use StorageTokenTrait;
        };
    }

    public function testSetAndGetStorageToken(): void
    {
        $token = 'test-token-123';
        $obj = $this->createObject();

        self::assertTrue(method_exists($obj, 'setStorageToken'));
        self::assertTrue(method_exists($obj, 'getStorageToken'));

        $obj->setStorageToken($token);
        self::assertSame($token, $obj->getStorageToken());
    }
}
