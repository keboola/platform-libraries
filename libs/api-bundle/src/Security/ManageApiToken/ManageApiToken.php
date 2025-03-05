<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Security\ManageApiToken;

use Keboola\ApiBundle\Security\TokenInterface;

class ManageApiToken implements TokenInterface
{
    public function __construct(
        private readonly int $id,
        private readonly string $description,
        /** @var list<string> */ private readonly array $scopes,
        /** @var list<string> */ private readonly array $features,
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
     * @return self
     */
    public static function fromVerifyResponse(array $data): self
    {
        return new self(
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
