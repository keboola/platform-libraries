<?php

declare(strict_types=1);

namespace Keboola\PersmissionChecker\Tests\Check\Scheduler;

use Keboola\PermissionChecker\Check\Scheduler\CanActivateSchedules;
use Keboola\PermissionChecker\Exception\PermissionDeniedException;
use Keboola\PermissionChecker\StorageApiToken;
use PHPUnit\Framework\TestCase;

class CanActivateSchedulesTest extends TestCase
{
    public static function provideValidPermissionsCheckData(): iterable
    {
        yield 'share role' => [
            'token' => new StorageApiToken(role: 'share'),
        ];

        yield 'admin role' => [
            'token' => new StorageApiToken(role: 'admin'),
        ];
    }

    /** @dataProvider provideValidPermissionsCheckData */
    public function testValidPermissionsCheck(
        StorageApiToken $token,
    ): void {
        $this->expectNotToPerformAssertions();

        $checker = new CanActivateSchedules();
        $checker->checkPermissions($token);
    }

    public static function provideInvalidPermissionsCheckData(): iterable
    {
        yield 'guest role' => [
            'token' => new StorageApiToken(role: 'guest'),
            'error' => new PermissionDeniedException('Role "guest" is insufficient for this operation.'),
        ];

        yield 'readOnly role' => [
            'token' => new StorageApiToken(role: 'readOnly'),
            'error' => new PermissionDeniedException('Role "readOnly" is insufficient for this operation.'),
        ];

        yield 'regular token' => [
            'token' => new StorageApiToken(),
            'error' => new PermissionDeniedException('Role "none" is insufficient for this operation.'),
        ];
    }

    /** @dataProvider provideInvalidPermissionsCheckData */
    public function testInvalidPermissionsCheck(
        StorageApiToken $token,
        PermissionDeniedException $error,
    ): void {
        $this->expectExceptionObject($error);

        $checker = new CanActivateSchedules();
        $checker->checkPermissions($token);
    }
}
