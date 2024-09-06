<?php

declare(strict_types=1);

namespace Keboola\PersmissionChecker\Tests;

use Keboola\PermissionChecker\Feature;
use Keboola\PermissionChecker\Role;
use Keboola\PermissionChecker\StorageApiToken;
use Keboola\PermissionChecker\TokenPermission;
use Keboola\StorageApiBranch\StorageApiToken as BaseStorageApiToken;
use PHPUnit\Framework\TestCase;
use ValueError;

class StorageApiTokenTest extends TestCase
{
    public function testHasFeature(): void
    {
        $token = new StorageApiToken(
            features: ['queuev2', 'invalid-feature'],
        );
        self::assertTrue($token->hasFeature(Feature::QUEUE_V2));
        self::assertFalse($token->hasFeature(Feature::PROTECTED_DEFAULT_BRANCH));
    }

    public function testGetRole(): void
    {
        // no role
        $token = new StorageApiToken(
            role: null,
        );
        self::assertSame(Role::NONE, $token->getRole());
        self::assertTrue($token->isRole(Role::NONE));
        self::assertTrue($token->isOneOfRoles([Role::NONE, Role::READ_ONLY]));
        self::assertFalse($token->isOneOfRoles([Role::READ_ONLY]));

        // valid role
        $token = new StorageApiToken(
            role: 'readOnly',
        );
        self::assertSame(Role::READ_ONLY, $token->getRole());
        self::assertTrue($token->isRole(Role::READ_ONLY));
        self::assertTrue($token->isOneOfRoles([Role::NONE, Role::READ_ONLY]));
        self::assertFalse($token->isOneOfRoles([Role::SHARE]));

        // invalid role
        $token = new StorageApiToken(
            role: 'invalid-role',
        );
        $this->expectException(ValueError::class);
        $this->expectExceptionMessage('"invalid-role" is not a valid backing value');
        $token->getRole();
    }

    public function testHasAllowedComponent(): void
    {
        // no allowed components list
        $token = new StorageApiToken(
            allowedComponents: null,
        );
        self::assertTrue($token->hasAllowedComponent('component-1'));

        // empty allowed components list
        $token = new StorageApiToken(
            allowedComponents: [],
        );
        self::assertFalse($token->hasAllowedComponent('component-1'));

        // explicit allowed components list
        $token = new StorageApiToken(
            allowedComponents: ['component-1', 'component-2'],
        );
        self::assertTrue($token->hasAllowedComponent('component-1'));
        self::assertFalse($token->hasAllowedComponent('component-3'));
    }

    public function testFromTokenInterface(): void
    {
        $token = StorageApiToken::fromTokenInterface(new class extends BaseStorageApiToken {
            public function __construct()
            {
                parent::__construct([], '');
            }

            public function getFeatures(): array
            {
                return ['queuev2', 'invalid-feature'];
            }

            public function getRole(): ?string
            {
                return 'readOnly';
            }

            public function getAllowedComponents(): ?array
            {
                return ['component-1', 'component-2'];
            }

            public function getPermissions(): array
            {
                return ['canCreateJobs', 'canManageBuckets'];
            }

            public function getProjectId(): string
            {
                return '123';
            }
        });

        self::assertTrue($token->hasFeature(Feature::QUEUE_V2));
        self::assertFalse($token->hasFeature(Feature::PROTECTED_DEFAULT_BRANCH));
        self::assertTrue($token->isRole(Role::READ_ONLY));
        self::assertTrue($token->hasAllowedComponent('component-1'));
        self::assertFalse($token->hasAllowedComponent('component-3'));
        self::assertTrue($token->hasPermission(TokenPermission::CAN_CREATE_JOBS));
        self::assertFalse($token->hasPermission(TokenPermission::CAN_MANAGE_TOKENS));
        self::assertSame([TokenPermission::CAN_CREATE_JOBS], $token->getPermissions());
        self::assertTrue($token->isForProject('123'));
        self::assertFalse($token->isForProject('456'));
    }

    public function testIsForProject(): void
    {
        $token = new StorageApiToken(
            projectId: '123',
        );
        self::assertTrue($token->isForProject('123'));
        self::assertFalse($token->isForProject('456'));
    }
}
