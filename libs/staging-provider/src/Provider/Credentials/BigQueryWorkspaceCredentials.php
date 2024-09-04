<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Provider\Credentials;

use Keboola\StagingProvider\Exception\StagingProviderException;
use Throwable;

class BigQueryWorkspaceCredentials implements CredentialsInterface
{
    private function __construct(
        private readonly string $type,
        private readonly string $project_id,
        private readonly string $private_key_id,
        private readonly string $client_email,
        private readonly string $client_id,
        private readonly string $auth_uri,
        private readonly string $token_uri,
        private readonly string $auth_provider_x509_cert_url,
        private readonly string $client_x509_cert_url,
        private readonly string $universe_domain,
        private readonly string $private_key,
    ) {
    }

    public static function fromPasswordResetArray(array $credentials): self
    {
        // This reads the response from the password reset endpoint for BigQuery backend
        // https://keboola.docs.apiary.io/#reference/workspaces/password-reset/password-reset
        try {
            return new self(
                $credentials['credentials']['type'],
                $credentials['credentials']['project_id'],
                $credentials['credentials']['private_key_id'],
                $credentials['credentials']['client_email'],
                $credentials['credentials']['client_id'],
                $credentials['credentials']['auth_uri'],
                $credentials['credentials']['token_uri'],
                $credentials['credentials']['auth_provider_x509_cert_url'],
                $credentials['credentials']['client_x509_cert_url'],
                $credentials['credentials']['universe_domain'],
                $credentials['credentials']['private_key'],
            );
        } catch (Throwable $e) {
            throw new StagingProviderException(
                sprintf(
                    'Invalid password reset response for BigQuery backend "%s" - keys: %s',
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
        return [
            'credentials' => [
                'type' => $this->type,
                'project_id' => $this->project_id,
                'private_key_id' => $this->private_key_id,
                'client_email' => $this->client_email,
                'client_id' => $this->client_id,
                'auth_uri' => $this->auth_uri,
                'token_uri' => $this->token_uri,
                'auth_provider_x509_cert_url' => $this->auth_provider_x509_cert_url,
                'client_x509_cert_url' => $this->client_x509_cert_url,
                'universe_domain' => $this->universe_domain,
                'private_key' => $this->private_key,
            ],
        ];
    }
}
