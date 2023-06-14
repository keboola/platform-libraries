<?php

declare(strict_types=1);

namespace Keboola\PermissionChecker;

class StorageApiToken
{
    public function __construct(
        private readonly array $features = [],
        private readonly ?string $role = null,
        private readonly ?array $allowedComponents = null,
    ) {
    }

    public static function fromTokenInterface(StorageApiTokenInterface $token): self
    {
        return new self(
            $token->getFeatures(),
            $token->getRole(),
            $token->getAllowedComponents(),
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
}
