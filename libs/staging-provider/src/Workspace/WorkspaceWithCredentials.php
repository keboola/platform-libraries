<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Workspace;

use Keboola\StagingProvider\Exception\StagingProviderException;
use Keboola\StagingProvider\Workspace\Configuration\WorkspaceCredentials;
use Keboola\StorageApi\WorkspaceLoginType;
use SensitiveParameter;
use Throwable;

/**
 * @phpstan-type CredentialsArray array{
 *       container?: string|null,
 *       connectionString?: string|null,
 *       host?: string|null,
 *       warehouse?: string|null,
 *       database?: string|null,
 *       schema?: string|null,
 *       user?: string|null,
 *       password?: string|null,
 *       privateKey?: string|null,
 *       account?: string|null,
 *       credentials?: array|null,
 *  }
 */
class WorkspaceWithCredentials implements WorkspaceWithCredentialsInterface
{
    public function __construct(
        private readonly WorkspaceInterface $workspace,
        private readonly WorkspaceCredentials $credentials,
    ) {
    }

    public static function createFromData(
        #[SensitiveParameter] array $workspaceData,
    ): self {
        try {
            return new self(
                workspace: Workspace::createFromData($workspaceData),
                credentials: WorkspaceCredentials::fromData($workspaceData['connection']),
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
        return $this->workspace->getWorkspaceId();
    }

    public function getBackendType(): string
    {
        return $this->workspace->getBackendType();
    }

    public function getBackendSize(): ?string
    {
        return $this->workspace->getBackendSize();
    }

    public function getLoginType(): WorkspaceLoginType
    {
        return $this->workspace->getLoginType();
    }

    public function getCredentials(): array
    {
        return $this->credentials->toArray();
    }
}
