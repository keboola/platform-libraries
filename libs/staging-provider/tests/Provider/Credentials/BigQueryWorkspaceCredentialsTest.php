<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Tests\Provider\Credentials;

use Keboola\StagingProvider\Exception\StagingProviderException;
use Keboola\StagingProvider\Provider\Credentials\BigQueryWorkspaceCredentials;
use PHPUnit\Framework\TestCase;

class BigQueryWorkspaceCredentialsTest extends TestCase
{
    public function testCreateSuccess(): void
    {
        $response = [
            'credentials' => [
                'type' => 'service_account',
                'project_id' => 'sapi-9866',
                'private_key_id' => '77xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
                'client_email' => 'sapi-xxxxxxxxxxxxxxxxxxxx@sapi-9866.iam.gserviceaccount.com',
                'client_id' => '11xxxxxxxxxxxxxxxxxxx',
                'auth_uri' => 'https://accounts.google.com/o/oauth2/auth',
                'token_uri' => 'https://oauth2.googleapis.com/token',
                'auth_provider_x509_cert_url' => 'https://www.googleapis.com/oauth2/v1/certs',
                'client_x509_cert_url' => 'https://www.googleapis.com/robot/v1/metadata/x509/sapi-workspacexxxx',
                'universe_domain' => 'googleapis.com',
                'private_key' => '-----BEGIN PRIVATE KEY-----\nMxxxxxxxxxxxxxxxxxxxxxs=\n-----END PRIVATE KEY-----\n',
            ],
        ];
        $credentials = BigQueryWorkspaceCredentials::fromPasswordResetArray($response);
        self::assertSame($response, $credentials->toArray());
    }

    public function testCreateFailure(): void
    {
        $response = [
            'password' => 'BlobEndpoint=https://kbcfshcwhatever',
        ];
        // phpcs:ignore Generic.Files.LineLength.MaxExceeded
        $this->expectExceptionMessage('Invalid password reset response for BigQuery backend "Undefined array key "credentials"" - keys: ["password"]');
        $this->expectException(StagingProviderException::class);
        BigQueryWorkspaceCredentials::fromPasswordResetArray($response);
    }
}
