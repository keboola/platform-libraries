<?php

declare(strict_types=1);

namespace Keboola\PersmissionChecker\Tests;

use Keboola\PermissionChecker\PermissionChecker;
use Keboola\PermissionChecker\PermissionCheckInterface;
use Keboola\PermissionChecker\StorageApiToken;
use Keboola\StorageApiBranch\StorageApiToken as BaseStorageApiToken;
use PHPUnit\Framework\TestCase;

class PermissionCheckerTest extends TestCase
{
    public function testCheckPermissions(): void
    {
        $token = new class extends BaseStorageApiToken {
            public function __construct()
            {
                parent::__construct([], '');
            }

            public function getFeatures(): array
            {
                return ['queuev2'];
            }

            public function getRole(): ?string
            {
                return null;
            }

            public function getAllowedComponents(): ?array
            {
                return null;
            }

            public function getPermissions(): array
            {
                return [];
            }

            public function getProjectId(): string
            {
                return '123';
            }
        };

        $expectedInternalToken = StorageApiToken::fromTokenInterface($token);

        $checker = $this->createMock(PermissionCheckInterface::class);
        $checker->expects(self::once())->method('checkPermissions')->with($expectedInternalToken);

        $permissionChecker = new PermissionChecker();
        $permissionChecker->checkPermissions($token, $checker);
    }
}
