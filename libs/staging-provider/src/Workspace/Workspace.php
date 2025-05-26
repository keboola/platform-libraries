<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Workspace;

use Keboola\StagingProvider\Exception\StagingProviderException;
use Keboola\StorageApi\WorkspaceLoginType;
use Throwable;

class Workspace implements WorkspaceInterface
{
    public function __construct(
        private readonly string $id,
        private readonly string $backendType,
        private readonly ?string $backendSize,
        private readonly WorkspaceLoginType $loginType,
    ) {
    }

    public static function createFromData(array $workspaceData): self
    {
        try {
            return new self(
                id: (string) $workspaceData['id'],
                backendType: $workspaceData['connection']['backend'],
                backendSize: $workspaceData['backendSize'] ?? null,
                loginType: WorkspaceLoginType::from(
                    $workspaceData['connection']['loginType'] ?? WorkspaceLoginType::DEFAULT->value,
                ),
            );
        } catch (Throwable $e) {
            throw new StagingProviderException(
                'Invalid workspace data: ' . $e->getMessage(),
                previous: $e,
            );
        }
    }

    public function getWorkspaceId(): string
    {
        return $this->id;
    }

    public function getBackendType(): string
    {
        return $this->backendType;
    }

    public function getBackendSize(): ?string
    {
        return $this->backendSize;
    }

    public function getLoginType(): WorkspaceLoginType
    {
        return $this->loginType;
    }
}
