<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Security\StorageApiToken;

use Keboola\ApiBundle\Security\TokenInterface;

class StorageApiToken implements TokenInterface
{
    public function __construct(
        private readonly array $tokenInfo,
        private readonly string $tokenValue,
    ) {
    }

    public function getTokenInfo(): array
    {
        return $this->tokenInfo;
    }

    public function getTokenValue(): string
    {
        return $this->tokenValue;
    }

    public function getProjectId(): string
    {
        return (string) $this->tokenInfo['owner']['id'];
    }

    public function getTokenId(): string
    {
        return (string) $this->tokenInfo['id'];
    }

    /**
     * @return list<string>
     */
    public function getFeatures(): array
    {
        return $this->tokenInfo['owner']['features'];
    }

    public function hasFeature(string $feature): bool
    {
        return in_array($feature, $this->getFeatures(), true);
    }

    public function getPayAsYouGoPurchasedCredits(): float
    {
        return (float) ($this->tokenInfo['owner']['payAsYouGo']['purchasedCredits'] ?? 0.0);
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
        return $this->tokenInfo['description'];
    }

    public function getUserIdentifier(): string
    {
        return $this->getTokenId();
    }
}
