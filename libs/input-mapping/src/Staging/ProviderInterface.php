<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Staging;

interface ProviderInterface
{
    public function getWorkspaceId(): string;

    public function cleanup(): void;

    /**
     * @return array{
     *      container?: string,
     *      connectionString?: string,
     *      host?: string,
     *      warehouse?: string,
     *      database?: string,
     *      schema?: string,
     *      user?: string,
     *      password?: string,
     *      account?: string,
     * }
     */
    public function getCredentials(): array;

    public function getPath(): string;
}
