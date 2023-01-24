<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Staging\Workspace;

class RedshiftWorkspaceStaging extends WorkspaceStaging
{
    public static function getType(): string
    {
        return 'redshift';
    }
}
