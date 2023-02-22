<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Tests\Authentication;

use Keboola\AzureApiClient\Authentication\TokenResponse;
use Keboola\AzureApiClient\Exception\ClientException;
use PHPUnit\Framework\TestCase;

class TokenResponseTest extends TestCase
{
    public function testFromResponseData(): void
    {
        // https://learn.microsoft.com/en-us/azure/active-directory/develop/v2-oauth2-auth-code-flow#successful-response-2
        $data = [
            'access_token' => 'access-token',
            'expires_in' => '3599',
        ];

        $response = TokenResponse::fromResponseData($data);

        self::assertSame('access-token', $response->accessToken);
    }

    /** @dataProvider provideInvalidResponseData */
    public function testFromInvalidResponseData(array $data, string $expectedError): void
    {
        $this->expectException(ClientException::class);
        $this->expectExceptionMessage($expectedError);

        $response = TokenResponse::fromResponseData($data);
    }

    public function provideInvalidResponseData(): iterable
    {
        yield 'no access token' => [
            'data' => [],
            'error' => 'Missing or invalid "access_token" in response: []',
        ];

        yield 'invalid access token' => [
            'data' => [
                'access_token' => [],
            ],
            'error' => 'Missing or invalid "access_token" in response: {"access_token":[]}',
        ];
    }
}
