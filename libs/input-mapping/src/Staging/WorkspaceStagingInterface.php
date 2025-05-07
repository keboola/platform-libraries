<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Staging;

interface WorkspaceStagingInterface extends StagingInterface
{
    public function getWorkspaceId(): string;

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

    public function cleanup(): void;
}
