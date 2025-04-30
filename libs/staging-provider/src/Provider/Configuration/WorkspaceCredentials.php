<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Provider\Configuration;

use SensitiveParameter;

class WorkspaceCredentials
{
    public function __construct(
        #[SensitiveParameter] public readonly array $credentials,
    ) {
    }
}
