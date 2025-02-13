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
        private readonly bool $isSuperAdmin,
        /** @var list<string> */ private readonly array $features,
    ) {
    }

    public static function fromVerifyResponse(array $data): self
    {
        return new self(
            $data['id'],
            $data['description'],
            $data['scopes'],
            $data['user']['isSuperAdmin'] ?? false,
            $data['user']['features'] ?? [],
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

    /**
     * @return list<string>
     */
    public function getFeatures(): array
    {
        return $this->features;
    }

    public function hasFeature(string $feature): bool
    {
        return in_array($feature, $this->features, true);
    }

    public function getRoles(): array
    {
        return [];
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
