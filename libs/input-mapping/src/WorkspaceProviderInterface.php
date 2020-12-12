<?php

namespace Keboola\InputMapping;

interface WorkspaceProviderInterface
{
    const TYPE_SNOWFLAKE = 'snowflake';
    const TYPE_REDSHIFT = 'redshift';
    const TYPE_SYNAPSE = 'synapse';
    const TYPE_ABS = 'abs';

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
