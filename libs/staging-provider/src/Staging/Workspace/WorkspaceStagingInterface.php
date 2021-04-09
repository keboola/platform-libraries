<?php

namespace Keboola\WorkspaceProvider\Staging\Workspace;

use Keboola\WorkspaceProvider\Staging\StagingInterface;

interface WorkspaceStagingInterface extends StagingInterface
{
    /**
     * @param array $data
     */
    public function __construct(array $data);

    /**
     * @return string
     */
    public function getWorkspaceId();

    /**
     * @return array
     */
    public function getCredentials();
}
