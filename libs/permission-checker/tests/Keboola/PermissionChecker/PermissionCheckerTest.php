<?php

declare(strict_types=1);

namespace Keboola\PermissionChecker;

use PHPUnit\Framework\TestCase;

class PermissionCheckerTest extends TestCase
{
    public function testCheckPermissions(): void
    {
        $token = new class implements StorageApiTokenInterface {
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
        };

        $expectedInternalToken = StorageApiToken::fromTokenInterface($token);

        $checker = $this->createMock(PermissionCheckerInterface::class);
        $checker->expects(self::once())->method('checkPermissions')->with($expectedInternalToken);

        $permissionChecker = new PermissionChecker();
        $permissionChecker->checkPermissions($token, $checker);
    }
}
