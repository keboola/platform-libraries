<?php

declare(strict_types=1);

namespace Keboola\PermissionChecker\Check\SandboxesService;

use Keboola\PermissionChecker\BranchType;
use Keboola\PermissionChecker\Exception\PermissionDeniedException;
use Keboola\PermissionChecker\Feature;
use Keboola\PermissionChecker\PermissionCheckInterface;
use Keboola\PermissionChecker\Role;
use Keboola\PermissionChecker\StorageApiToken;

/**
 * Issuing a repository access token grants write access to the repository the app deploys from, so on top of
 * the admin/project rule of CanManageAppRepoCredentials the token's role must be allowed to write to the
 * app's branch. A null branch type means the default branch.
 */
class CanIssueAppRepoAccessToken implements PermissionCheckInterface
{
    private readonly CanManageAppRepoCredentials $canManageAppRepoCredentials;

    public function __construct(
        private readonly int $appId,
        string $appProjectId,
        private readonly ?BranchType $branchType,
    ) {
        $this->canManageAppRepoCredentials = new CanManageAppRepoCredentials($appId, $appProjectId);
    }

    public function checkPermissions(StorageApiToken $token): void
    {
        $this->canManageAppRepoCredentials->checkPermissions($token);

        if (!$this->isRoleAllowed($token)) {
            throw PermissionDeniedException::roleDenied(
                $token->getRole(),
                sprintf('issue a repository access token for app \'%s\'', $this->appId),
            );
        }
    }

    private function isRoleAllowed(StorageApiToken $token): bool
    {
        if ($token->hasFeature(Feature::PROTECTED_DEFAULT_BRANCH)) {
            return match ($token->getRole()) {
                Role::PRODUCTION_MANAGER => $this->branchType === null || $this->branchType === BranchType::DEFAULT,
                Role::DEVELOPER, Role::REVIEWER => $this->branchType === BranchType::DEV,
                default => false,
            };
        }

        return match ($token->getRole()) {
            Role::NONE, Role::READ_ONLY => false,
            default => true,
        };
    }
}
