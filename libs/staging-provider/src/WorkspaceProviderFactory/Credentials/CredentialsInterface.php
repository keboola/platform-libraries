<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\WorkspaceProviderFactory\Credentials;

interface CredentialsInterface
{
    public static function fromPasswordResetArray(array $credentials): self;
    public function toArray(): array;
}
