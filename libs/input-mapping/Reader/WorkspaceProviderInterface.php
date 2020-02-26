<?php

namespace Keboola\InputMapping\Reader;

interface WorkspaceProviderInterface
{
    const TYPE_SNOWFLAKE = 'snowflake';
    const TYPE_REDSHIFT = 'redshift';

    public function getWorkspaceId($type);
}
