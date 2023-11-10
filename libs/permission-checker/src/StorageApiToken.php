<?php

declare(strict_types=1);

namespace Keboola\PermissionChecker;

use Keboola\StorageApiBranch\StorageApiToken as BaseStorageApiToken;

class StorageApiToken
{
    public function __construct(
        private readonly array $features = [],
        private readonly ?string $role = null,
        private readonly ?array $allowedComponents = null,
        private readonly array $permissions = [],
    ) {
    }

    public static function fromTokenInterface(BaseStorageApiToken $token): self
    {
        return new self(
            $token->getFeatures(),
            $token->getRole(),
            $token->getAllowedComponents(),
            $token->getPermissions(),
        );
    }

    public function hasFeature(Feature $feature): bool
    {
        return in_array($feature->value, $this->features, true);
    }

    public function getRole(): Role
    {
        return $this->role ? Role::from($this->role) : Role::NONE;
    }

    public function isRole(Role $role): bool
    {
        return $this->getRole() === $role;
    }

    /**
     * @param Role[] $roles
     */
    public function isOneOfRoles(array $roles): bool
    {
        return in_array($this->getRole(), $roles, true);
    }

    public function hasAllowedComponent(string $componentId): bool
    {
        return $this->allowedComponents === null || in_array($componentId, $this->allowedComponents, true);
    }

    /**
     * @return TokenPermission[]
     */
    public function getPermissions(): array
    {
        return array_filter(
            array_map(
                function (string $value) {
                    return TokenPermission::tryFrom($value);
                },
                $this->permissions
            ),
            function (?TokenPermission $permission) {
                return $permission !== null;
            }
        );
    }

    public function hasPermission(TokenPermission $permission): bool
    {
        return in_array($permission, $this->getPermissions());
    }
}
