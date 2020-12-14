<?php

namespace Keboola\InputMapping\Staging;

interface ProviderInterface
{
    /**
     * @return string
     */
    public function getWorkspaceId();

    public function cleanup();

    /**
     * @return array
     */
    public function getCredentials();

    /**
     * @return string
     */
    public function getPath();
}
