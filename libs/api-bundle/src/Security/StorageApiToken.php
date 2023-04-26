<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Security;

use Symfony\Component\Security\Core\User\UserInterface;

class StorageApiToken implements UserInterface
{
    private array $tokenInfo;
    private string $tokenString;

    public function __construct(array $tokenInfo, string $tokenString)
    {
        $this->tokenInfo = $tokenInfo;
        $this->tokenString = $tokenString;
    }

    public function getTokenInfo(): array
    {
        return $this->tokenInfo;
    }

    public function getProjectId(): string
    {
        return (string) $this->tokenInfo['owner']['id'];
    }

    public function getTokenId(): string
    {
        return (string) $this->tokenInfo['id'];
    }

    public function getFileStorageProvider(): string
    {
        return $this->tokenInfo['owner']['fileStorageProvider'];
    }

    public function getProjectName(): string
    {
        return $this->tokenInfo['owner']['name'];
    }

    public function getTokenDesc(): string
    {
        return $this->tokenInfo['description'];
    }

    public function getRole(): ?string
    {
        return $this->tokenInfo['admin']['role'] ?? null;
    }

    public function getSamlUserId(): ?string
    {
        return $this->tokenInfo['admin']['samlParameters']['userId'] ?? null;
    }

    public function isReadOnlyRole(): bool
    {
        return strtolower($this->getRole() ?? '') === 'readonly';
    }

    public function isAllowedToUseComponent(string $componentId): bool
    {
        $componentAccess = $this->tokenInfo['componentAccess'] ?? [];
        return empty($componentAccess) || in_array($componentId, $componentAccess, true);
    }

    public function getTokenString(): string
    {
        return $this->tokenString;
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
        return '';
    }

    public function getUserIdentifier(): string
    {
        return $this->getUsername();
    }

    public function getFeatures(): array
    {
        return $this->tokenInfo['owner']['features'];
    }

    public function hasFeature(string $feature): bool
    {
        return in_array($feature, $this->getFeatures(), true);
    }
}
