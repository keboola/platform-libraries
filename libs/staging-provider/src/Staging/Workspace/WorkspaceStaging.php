<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Staging\Workspace;

use Keboola\StagingProvider\Exception\StagingProviderException;

abstract class WorkspaceStaging implements WorkspaceStagingInterface
{
    protected array $data;

    public function __construct(array $data)
    {
        if ($data['connection']['backend'] !== static::getType()) {
            throw new StagingProviderException(sprintf(
                'Backend configuration does not match the workspace type. Expected "%s", got "%s"',
                static::getType(),
                $data['connection']['backend']
            ));
        }

        $this->data = $data;
    }

    public function getWorkspaceId(): string
    {
        return (string) $this->data['id'];
    }

    public function getCredentials(): array
    {
        $connection = $this->data['connection'];

        return [
            'host' => $connection['host'],
            'warehouse' => $connection['warehouse'],
            'database' => $connection['database'],
            'schema' => $connection['schema'],
            'user' => $connection['user'],
            'password' => $connection['password'],
        ];
    }

    public function getBackendSize(): ?string
    {
        return $this->data['backendSize'];
    }
}
