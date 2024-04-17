<?php

declare(strict_types=1);

namespace Keboola\SandboxesServiceApiClient\Tests\Sandboxes\Legacy;

use InvalidArgumentException;
use Keboola\SandboxesServiceApiClient\Sandboxes\Legacy\SandboxCredentials;
use PHPUnit\Framework\TestCase;

class SandboxCredentialsTest extends TestCase
{
    public function testFromArrayToArray(): void
    {
        $input = [
            'type' => 'service_account',
            'project_id' => '23432',
            'private_key_id' => '324',
            'client_email' => '234',
            'client_id' => '2342',
            'auth_uri' => 'https://accounts.google.com/o/oauth2/auth',
            'token_uri' => 'https://oauth2.googleapis.com/token',
            'auth_provider_x509_cert_url' => 'https://www.googleapis.com/oauth2/v1/certs',
            'client_x509_cert_url' => 'https://www.googleapis.com/robot/v1/metadata/x509.com',
            'private_key' => '-----BEGIN PRIVATE KEY-----key-----END PRIVATE KEY-----',
        ];
        $credentials = SandboxCredentials::fromArray($input);

        self::assertSame($input, $credentials->toArray());
    }

    public function testFromArrayInvalidValues(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'Missing credential field(s) "type,project_id,private_key_id,client_email,token_uri,' .
            'auth_provider_x509_cert_url,client_x509_cert_url,private_key"',
        );
        SandboxCredentials::fromArray([
            'something' => 'weird',
            'client_id' => '1234',
            'auth_uri' => 'https://accounts.google.com/o/oauth2/auth',
        ]);
    }
}
