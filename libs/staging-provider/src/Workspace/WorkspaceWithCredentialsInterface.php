<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Workspace;

interface WorkspaceWithCredentialsInterface extends WorkspaceInterface
{
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
