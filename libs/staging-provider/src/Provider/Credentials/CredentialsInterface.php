<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Provider\Credentials;

interface CredentialsInterface
{
    public function toArray(): array;
    public static function fromPasswordResetArray(array $credentials): self;
}
