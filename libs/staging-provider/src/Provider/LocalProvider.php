<?php

namespace Keboola\WorkspaceProvider\Provider;

use Keboola\InputMapping\Staging\ProviderInterface;
use Keboola\WorkspaceProvider\Exception\WorkspaceProviderException;

class LocalProvider implements ProviderInterface
{
    /** @var string */
    protected $path;

    /**
     * @param string $path
     */
    public function __construct($path)
    {
        $this->path = $path;
    }

    public function getWorkspaceId()
    {
        throw new WorkspaceProviderException('Local provider has no workspace');
    }

    public function cleanup()
    {
    }

    public function getCredentials()
    {
        throw new WorkspaceProviderException('Local provider has no workspace');
    }

    public function getPath()
    {
        return $this->path;
    }
}
