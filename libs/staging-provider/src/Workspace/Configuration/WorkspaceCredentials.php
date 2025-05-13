<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Workspace\Configuration;

use SensitiveParameter;

class WorkspaceCredentials
{
    public function __construct(
        #[SensitiveParameter] public readonly array $credentials,
    ) {
    }
}
