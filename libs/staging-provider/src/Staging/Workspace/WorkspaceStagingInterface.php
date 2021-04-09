<?php

namespace Keboola\StagingProvider\Staging\Workspace;

use Keboola\StagingProvider\Staging\StagingInterface;

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
