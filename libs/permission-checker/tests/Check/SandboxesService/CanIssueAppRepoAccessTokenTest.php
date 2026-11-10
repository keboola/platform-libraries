<?php

declare(strict_types=1);

namespace Keboola\PermissionChecker\Tests\Check\SandboxesService;

use Keboola\PermissionChecker\BranchType;
use Keboola\PermissionChecker\Check\SandboxesService\CanIssueAppRepoAccessToken;
use Keboola\PermissionChecker\Exception\PermissionDeniedException;
use Keboola\PermissionChecker\StorageApiToken;
use PHPUnit\Framework\TestCase;

class CanIssueAppRepoAccessTokenTest extends TestCase
{
    private const APP_ID = 1;
    private const PROJECT_ID = '123';

    /**
     * Expected outcome per role, as [default branch, dev branch, no branch].
     */
    private const EXPECTED_WITHOUT_PROTECTED_DEFAULT_BRANCH = [
        'none' => [false, false, false],
        'admin' => [true, true, true],
        'guest' => [true, true, true],
        'readOnly' => [false, false, false],
        'share' => [true, true, true],
        'developer' => [true, true, true],
        'reviewer' => [true, true, true],
        'productionManager' => [true, true, true],
    ];

    /**
     * Expected outcome per role, as [default branch, dev branch, no branch].
     */
    private const EXPECTED_WITH_PROTECTED_DEFAULT_BRANCH = [
        'none' => [false, false, false],
        'admin' => [false, false, false],
        'guest' => [false, false, false],
        'readOnly' => [false, false, false],
        'share' => [false, false, false],
        'developer' => [false, true, false],
        'reviewer' => [false, true, false],
        'productionManager' => [true, false, true],
    ];

    public static function provideRoleAndBranchData(): iterable
    {
        $variants = [
            'regular project' => [
                'features' => [],
                'expectations' => self::EXPECTED_WITHOUT_PROTECTED_DEFAULT_BRANCH,
            ],
            'protected default branch' => [
                'features' => ['protected-default-branch'],
                'expectations' => self::EXPECTED_WITH_PROTECTED_DEFAULT_BRANCH,
            ],
        ];
        $branches = [
            'default branch' => BranchType::DEFAULT,
            'dev branch' => BranchType::DEV,
            'no branch' => null,
        ];

        foreach ($variants as $variantName => ['features' => $features, 'expectations' => $expectations]) {
            foreach ($expectations as $role => $expectedPerBranch) {
                foreach (array_keys($branches) as $index => $branchName) {
                    yield sprintf('%s on %s, %s', $role, $variantName, $branchName) => [
                        'features' => $features,
                        'role' => $role === 'none' ? null : $role,
                        'branchType' => $branches[$branchName],
                        'isAllowed' => $expectedPerBranch[$index],
                    ];
                }
            }
        }
    }

    /**
     * @dataProvider provideRoleAndBranchData
     */
    public function testRoleAndBranch(
        array $features,
        ?string $role,
        ?BranchType $branchType,
        bool $isAllowed,
    ): void {
        if ($isAllowed) {
            $this->expectNotToPerformAssertions();
        } else {
            $this->expectExceptionObject(new PermissionDeniedException(sprintf(
                'Role "%s" is not allowed to issue a repository access token for app \'1\'',
                $role ?? 'none',
            )));
        }

        $checker = new CanIssueAppRepoAccessToken(self::APP_ID, self::PROJECT_ID, $branchType);
        $checker->checkPermissions(new StorageApiToken(
            features: $features,
            role: $role,
            projectId: self::PROJECT_ID,
            isAdminToken: true,
        ));
    }

    public function testDenyNonAdminToken(): void
    {
        $this->expectExceptionObject(new PermissionDeniedException(
            'Token is not authorized to manage credentials of app \'1\', admin context is required',
        ));

        $checker = new CanIssueAppRepoAccessToken(self::APP_ID, self::PROJECT_ID, BranchType::DEFAULT);
        $checker->checkPermissions(new StorageApiToken(
            role: 'admin',
            projectId: self::PROJECT_ID,
            isAdminToken: false,
        ));
    }

    public function testDenyDifferentProject(): void
    {
        $this->expectExceptionObject(new PermissionDeniedException(
            'Token is not authorized to manage credentials of app \'1\', app is from different project',
        ));

        $checker = new CanIssueAppRepoAccessToken(self::APP_ID, '456', BranchType::DEFAULT);
        $checker->checkPermissions(new StorageApiToken(
            role: 'admin',
            projectId: self::PROJECT_ID,
            isAdminToken: true,
        ));
    }
}
