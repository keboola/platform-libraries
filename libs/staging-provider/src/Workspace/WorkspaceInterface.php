<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Workspace;

use Keboola\StagingProvider\Staging\StagingInterface;

interface WorkspaceInterface extends StagingInterface
{
    public function getWorkspaceId(): string;

    public function getBackendType(): string;

    public function getBackendSize(): ?string;

    /**
     * @return array{
     *      container?: string|null,
     *      connectionString?: string|null,
     *      host?: string|null,
     *      warehouse?: string|null,
     *      database?: string|null,
     *      schema?: string|null,
     *      user?: string|null,
     *      password?: string|null,
     *      privateKey?: string|null,
     *      account?: string|null,
     *      credentials?: array|null,
     * }
     */
    public function getCredentials(): array;
}
