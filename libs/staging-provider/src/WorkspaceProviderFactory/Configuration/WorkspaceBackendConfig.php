<?php

namespace Keboola\StagingProvider\WorkspaceProviderFactory\Configuration;

class WorkspaceBackendConfig
{
    /** @var null|string */
    private $type;

    /**
     * @param string|null $type
     */
    public function __construct($type)
    {
        $this->type = $type;
    }

    /**
     * @return string|null
     */
    public function getType()
    {
        return $this->type;
    }
}
