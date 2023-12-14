<?php

declare(strict_types=1);

namespace Keboola\PersmissionChecker\Tests\Check\RunnerSyncApi;

use Generator;
use Keboola\PermissionChecker\Check\RunnerSyncApi\CanResolveConfigVariables;
use Keboola\PermissionChecker\Exception\PermissionDeniedException;
use Keboola\PermissionChecker\StorageApiToken;
use PHPUnit\Framework\TestCase;

class CanResolveConfigVariablesTest extends TestCase
{
    public function testValidPermissionsCheck(): void
    {
        $this->expectNotToPerformAssertions();

        $token = new StorageApiToken(
            allowedComponents: [
                'keboola.shared-code',
                'keboola.variables',
            ],
        );

        $checker = new CanResolveConfigVariables();
        $checker->checkPermissions($token);
    }

    /** @dataProvider provideInvalidPermissionsCheckData */
    public function testInvalidPermissionsCheck(
        ?array $allowedComponents,
        string $errorMessage,
    ): void {
        $token = new StorageApiToken(allowedComponents: $allowedComponents);

        $this->expectException(PermissionDeniedException::class);
        $this->expectExceptionMessage($errorMessage);

        $checker = new CanResolveConfigVariables();
        $checker->checkPermissions($token);
    }

    public static function provideInvalidPermissionsCheckData(): Generator
    {
        yield 'for any component' => [
            [],
            'You do not have permission to read configurations of "keboola.shared-code" component',
        ];

        yield 'unnecessary component permission' => [
            ['keboola.orchestrator'],
            'You do not have permission to read configurations of "keboola.shared-code" component',
        ];

        yield 'only one of required permissions - shared-code' => [
            ['keboola.shared-code'],
            'You do not have permission to read configurations of "keboola.variables" component',
        ];

        yield 'only one of required permissions - variables' => [
            ['keboola.variables'],
            'You do not have permission to read configurations of "keboola.shared-code" component',
        ];
    }
}
