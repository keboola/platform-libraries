<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Workspace;

use SensitiveParameter;

readonly class WorkspaceCredentials
{
    public function __construct(
        #[SensitiveParameter] public array $credentials,
    ) {
    }
}
