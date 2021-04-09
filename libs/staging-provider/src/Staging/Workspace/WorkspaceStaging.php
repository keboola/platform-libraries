<?php

namespace Keboola\StagingProvider\Staging\Workspace;

use Keboola\StagingProvider\Exception\StagingProviderException;

abstract class WorkspaceStaging implements WorkspaceStagingInterface
{
    /** @var array */
    protected $data;

    /**
     * @param array $data
     */
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

    public function getWorkspaceId()
    {
        return $this->data['id'];
    }

    public function getCredentials()
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
}
