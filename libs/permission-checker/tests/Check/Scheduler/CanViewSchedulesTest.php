<?php

declare(strict_types=1);

namespace Keboola\PersmissionChecker\Tests\Check\Scheduler;

use Keboola\PermissionChecker\Check\Scheduler\CanViewSchedules;
use Keboola\PermissionChecker\StorageApiToken;
use PHPUnit\Framework\TestCase;

class CanViewSchedulesTest extends TestCase
{
    public static function provideValidPermissionsCheckData(): iterable
    {
        yield 'share role' => [
            'token' => new StorageApiToken(role: 'share'),
        ];

        yield 'admin role' => [
            'token' => new StorageApiToken(role: 'admin'),
        ];

        yield 'guest role' => [
            'token' => new StorageApiToken(role: 'guest'),
        ];

        yield 'readOnly role' => [
            'token' => new StorageApiToken(role: 'readOnly'),
        ];

        yield 'regular token' => [
            'token' => new StorageApiToken(),
        ];
    }

    /** @dataProvider provideValidPermissionsCheckData */
    public function testValidPermissionsCheck(
        StorageApiToken $token,
    ): void {
        $this->expectNotToPerformAssertions();

        $checker = new CanViewSchedules();
        $checker->checkPermissions($token);
    }
}
