<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Provider\Credentials;

use Keboola\StagingProvider\Exception\StagingProviderException;
use Throwable;

class DatabaseWorkspaceCredentials implements CredentialsInterface
{
    private function __construct(private readonly string $password)
    {
    }

    public static function fromPasswordResetArray(array $credentials): self
    {
        // This reads the response from the password reset endpoint for Redshift/Snowflake/Synapse backend
        // https://keboola.docs.apiary.io/#reference/workspaces/password-reset/password-reset
        try {
            return new self(
                $credentials['password'],
            );
        } catch (Throwable $e) {
            throw new StagingProviderException(
                sprintf(
                    'Invalid password reset response for DB backend "%s" - keys: %s',
                    $e->getMessage(),
                    implode((array) json_encode(array_keys($credentials))),
                ),
                $e->getCode(),
                $e,
            );
        }
    }

    public function toArray(): array
    {
        return ['password' => $this->password];
    }
}
