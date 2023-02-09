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
        // reverse-engineered from implementation
        $data = [
            'access_token' => 'access-token',
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
            'error' => 'Access token not provided in response: []',
        ];

        yield 'invalid access token' => [
            'data' => [
                'access_token' => [],
            ],
            'error' => 'Access token not provided in response: {"access_token":[]}',
        ];
    }
}
