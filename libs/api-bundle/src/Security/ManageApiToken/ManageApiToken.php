<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Security\ManageApiToken;

use Keboola\ApiBundle\Security\KubernetesServiceAccount\KubernetesServiceAccountToken;
use Keboola\ApiBundle\Security\TokenInterface;

/**
 * @deprecated Use {@see KubernetesServiceAccountToken} instead. Kept as the
 *     backwards-compatible supertype so existing `#[CurrentUser] ManageApiToken`
 *     type-hints keep accepting the token the authenticator produces.
 */
class ManageApiToken implements TokenInterface
{
    /**
     * @param list<string> $scopes
     * @param list<string> $features
     * @final
     */
    protected function __construct(
        private readonly int $id,
        private readonly string $description,
        private readonly array $scopes,
        private readonly array $features,
        private readonly bool $isSuperAdmin,
    ) {
    }

    /**
     * Structure of the token verification response from Manage API
     *
     * @param array{
     *     id: int,
     *     description: string,
     *     created: string,
     *     lastUsed: string|null,
     *     expires: string|null,
     *     isSessionToken: bool,
     *     isExpired: bool,
     *     isDisabled: bool,
     *     scopes: list<string>,
     *     type: string,
     *     creator: array{
     *         id: int|string,
     *         name: string
     *     },
     *     user?: array{
     *         id: int,
     *         name: string,
     *         email: string,
     *         mfaEnabled: bool,
     *         features: list<string>,
     *         canAccessLogs: bool,
     *         isSuperAdmin: bool
     *     }
     * } $data
     * @return static
     */
    public static function fromVerifyResponse(array $data): static
    {
        return new static(
            (int) $data['id'],
            (string) $data['description'],
            $data['scopes'],
            $data['user']['features'] ?? [],
            (bool) ($data['user']['isSuperAdmin'] ?? false),
        );
    }

    /**
     * @return list<string>
     */
    public function getScopes(): array
    {
        return $this->scopes;
    }

    public function hasScope(string $scope): bool
    {
        return in_array($scope, $this->scopes, true);
    }

    public function getRoles(): array
    {
        return [];
    }

    public function getFeatures(): array
    {
        return $this->features;
    }

    public function hasFeature(string $feature): bool
    {
        return in_array($feature, $this->features, true);
    }

    public function getPassword(): ?string
    {
        return null;
    }

    public function getSalt(): ?string
    {
        return null;
    }

    public function eraseCredentials(): void
    {
    }

    public function getUsername(): string
    {
        return $this->description;
    }

    public function getUserIdentifier(): string
    {
        return (string) $this->id;
    }

    public function isSuperAdmin(): bool
    {
        return $this->isSuperAdmin;
    }
}
