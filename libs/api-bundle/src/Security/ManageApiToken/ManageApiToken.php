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
    ) {
    }

    public static function fromVerifyResponse(array $data): self
    {
        return new self(
            $data['id'],
            $data['description'],
            $data['scopes'],
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
}
