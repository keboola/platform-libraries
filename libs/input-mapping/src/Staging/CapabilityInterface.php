<?php

namespace Keboola\InputMapping\Staging;

interface CapabilityInterface
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
