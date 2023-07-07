<?php

declare(strict_types=1);

namespace Keboola\PersmissionChecker\Tests\Check\RunnerSyncApi;

use Generator;
use Keboola\PermissionChecker\BranchType;
use Keboola\PermissionChecker\Check\RunnerSyncApi\CanRunAction;
use Keboola\PermissionChecker\Exception\PermissionDeniedException;
use Keboola\PermissionChecker\StorageApiToken;
use PHPUnit\Framework\TestCase;

class CanRunActionTest extends TestCase
{
    public function branchTypeProvider(): Generator
    {
        yield 'dev branch' => [BranchType::DEV];
        yield 'default branch' => [BranchType::DEFAULT];
    }

    public static function provideValidPermissionsCheckData(): Generator
    {
        /** @var (BranchType)[] $branchTypes */
        $branchTypes = [BranchType::DEFAULT, BranchType::DEV];
        $roles = [null, 'guest', 'readOnly', 'admin', 'share', 'developer', 'reviewer', 'productionManager'];
        $allowedComponentsOptions = [null, ['dummy-component']];

        // standard projects
        foreach ($branchTypes as $branchType) {
            foreach ($roles as $role) {
                if ($role === 'readOnly') {
                    continue;
                }

                $label = $role ?: 'regular token';
                $label .= sprintf(' on %s branch', $branchType->value);

                foreach ($allowedComponentsOptions as $allowedComponents) {
                    $providedDataLabel = $label;
                    $providedDataLabel .= $allowedComponents ? ' with all compoment access'
                        : ' with dummy-component access';
                    yield $providedDataLabel => [
                        'token' => new StorageApiToken(
                            role: $role,
                            allowedComponents: $allowedComponents
                        ),
                        'branchType' => $branchType,
                    ];
                }
            }
        }

        // sox projects
        yield 'sox productionManager role on default branch' => [
            'token' => new StorageApiToken(
                features: [
                    'protected-default-branch',
                ],
                role: 'productionManager'
            ),
            'branchType' => BranchType::DEFAULT,
        ];

        yield 'sox developer role on dev branch' => [
            'token' => new StorageApiToken(
                features: [
                    'protected-default-branch',
                ],
                role: 'developer'
            ),
            'branchType' => BranchType::DEV,
        ];

        yield 'sox reviewer role on dev branch' => [
            'token' => new StorageApiToken(
                features: [
                    'protected-default-branch',
                ],
                role: 'reviewer'
            ),
            'branchType' => BranchType::DEV,
        ];
    }

    /** @dataProvider provideValidPermissionsCheckData */
    public function testValidPermissionsCheck(
        StorageApiToken $token,
        BranchType $branchType
    ): void {
        $this->expectNotToPerformAssertions();

        $checker = new CanRunAction($branchType, 'dummy-component');
        $checker->checkPermissions($token);
    }

    public static function provideInvalidPermissionsCheckData(): Generator
    {
        $branchTypes = [BranchType::DEFAULT, BranchType::DEV];

        foreach ($branchTypes as $branchType) {
            $label = 'regular token without required component access';
            $label .= sprintf(' on %s branch', $branchType->value);

            yield $label => [
                'token' => new StorageApiToken(
                    allowedComponents: ['keboola.orchestrator']
                ),
                'branchType' => $branchType,
                'errorMessage' => 'Token is not allowed to run component "dummy-component"',
            ];

            $label = 'regular token without any component access';
            $label .= sprintf(' on %s branch', $branchType->value);

            yield $label => [
                'token' => new StorageApiToken(
                    allowedComponents: []
                ),
                'branchType' => $branchType,
                'errorMessage' => 'Token is not allowed to run component "dummy-component"',
            ];

            $label = 'readOnly';
            $label .= sprintf(' on %s branch', $branchType->value);

            yield $label => [
                'token' => new StorageApiToken(
                    role: 'readOnly',
                ),
                'branchType' => $branchType,
                'errorMessage' => 'Role "readOnly" is not allowed to run actions',
            ];
        }

        // sox projects
        $roles = [null, 'guest', 'readOnly', 'admin', 'share', 'developer', 'reviewer', 'productionManager'];

        foreach ($branchTypes as $branchType) {
            foreach ($roles as $role) {
                if ($role === 'productionManager' && $branchType === BranchType::DEFAULT) {
                    continue;
                }
                if (in_array($role, ['developer', 'reviewer']) && $branchType === BranchType::DEV) {
                    continue;
                }

                $label = 'sox ';
                $label .= $role ?: 'regular token';
                $label .= sprintf(' on %s branch', $branchType->value);

                yield $label => [
                    'token' => new StorageApiToken(
                        features: [
                            'protected-default-branch',
                        ],
                        role: $role
                    ),
                    'branchType' => $branchType,
                    'errorMessage' => sprintf(
                        'Role "%s" is not allowed to run actions on %s branch',
                        $role?: 'none',
                        $branchType->value
                    ),
                ];
            }
        }
    }

    /** @dataProvider provideInvalidPermissionsCheckData */
    public function testInvalidPermissionsCheck(
        StorageApiToken $token,
        BranchType $branchType,
        string $errorMessage,
    ): void {
        $this->expectException(PermissionDeniedException::class);
        $this->expectExceptionMessage($errorMessage);

        $checker = new CanRunAction($branchType, 'dummy-component');
        $checker->checkPermissions($token);
    }
}
