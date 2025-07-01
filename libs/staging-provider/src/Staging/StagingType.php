<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Staging;

enum StagingType: string
{
    case None = 'none';
    case Local = 'local';
    case Abs = 'abs';
    case S3 = 's3';

    case WorkspaceSnowflake = 'workspace-snowflake';
    case WorkspaceBigquery = 'workspace-bigquery';

    public function getStagingClass(): StagingClass
    {
        return match ($this) {
            self::None => StagingClass::None,

            self::Local,
            self::Abs,
            self::S3 => StagingClass::Disk,

            self::WorkspaceSnowflake,
            self::WorkspaceBigquery => StagingClass::Workspace,
        };
    }
}
