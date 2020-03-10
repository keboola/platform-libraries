<?php

namespace Keboola\InputMapping\Reader;

interface WorkspaceProviderInterface
{
    const TYPE_SNOWFLAKE = 'snowflake';
    const TYPE_REDSHIFT = 'redshift';

    /**
     * @param string $type
     * @return string
     */
    public function getWorkspaceId($type);

    public function cleanup();

    /**
     * @param string $type
     * @return array
     */
    public function getCredentials($type);
}
