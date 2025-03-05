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

    public static function fromVerifyResponse(array $data): self
    {
        assert(is_scalar($data['id']), 'Token ID must be an integer');
        assert(is_scalar($data['description']), 'Token description must be a string');
        assert(is_array($data['scopes']), 'Token scopes must be an array');
        $userData = $data['user'] ?? [];
        assert(
            is_array($userData),
            'Token user data must be an array',
        );
        $features = $userData['features'] ?? [];
        assert(
            is_array($features),
            'Token features must be an array if present',
        );
        assert(
            !isset($userData['isSuperAdmin']) || is_scalar($userData['isSuperAdmin']),
            'Token isSuperAdmin must be a scalar if present',
        );

        return new self(
            (int) $data['id'],
            (string) $data['description'],
            array_values(array_map(function ($scope) {
                assert(is_string($scope));
                return (string) $scope;
            }, $data['scopes'])),
            array_values(array_map(function ($feature) {
                assert(is_string($feature));
                return (string) $feature;
            }, $features)),
            (bool) ($userData['isSuperAdmin'] ?? false),
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
