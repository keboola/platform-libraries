<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Tests;

use Keboola\AzureApiClient\Authentication\AuthenticatorFactory;
use Keboola\AzureApiClient\Authentication\AuthenticatorInterface;
use Keboola\AzureApiClient\AzureApiClient;
use Keboola\AzureApiClient\GuzzleClientFactory;
use Monolog\Logger;
use PHPUnit\Framework\TestCase;

class AzureApiClientFunctionalTest extends TestCase
{
    public function testSendGetRequest(): void
    {
        $mockserver = new Mockserver();
        $mockserver->reset();
        $mockserver->expect([
            'httpRequest' => [
                'method' => 'GET',
                'path' => '/foo/bar',
                'headers' => [
                    'Authorization' => 'Bearer auth-token',
                ],
            ],
            'httpResponse' => [
                'statusCode' => 200,
                'body' => json_encode(['foo' => 'bar']),
            ],
        ]);

        $logger = new Logger('test');
        $authenticatorFactory = $this->createAuthenticatorFactory('auth-token');
        $guzzleClientFactory = new GuzzleClientFactory($logger);

        $apiClient = new AzureApiClient(
            $mockserver->getServerUrl(),
            'my-api',
            $guzzleClientFactory,
            $authenticatorFactory,
            $logger,
        );

        $response = $apiClient->sendRequest('GET', 'foo/bar');
        self::assertSame(['foo' => 'bar'], $response);
    }

    public function testSendPostRequestWithResponse(): void
    {
        $mockserver = new Mockserver();
        $mockserver->reset();
        $mockserver->expect([
            'httpRequest' => [
                'method' => 'POST',
                'path' => '/foo/bar',
                'headers' => [
                    'Authorization' => 'Bearer auth-token',
                    'Content-Type' => 'application/json',
                ],
                'body' => '{"foo":"baz"}'
            ],
            'httpResponse' => [
                'statusCode' => 200,
                'body' => json_encode(['foo' => 'bar']),
            ],
        ]);

        $logger = new Logger('test');
        $authenticatorFactory = $this->createAuthenticatorFactory('auth-token');
        $guzzleClientFactory = new GuzzleClientFactory($logger);

        $apiClient = new AzureApiClient(
            $mockserver->getServerUrl(),
            'my-api',
            $guzzleClientFactory,
            $authenticatorFactory,
            $logger,
        );

        $response = $apiClient->sendRequest(
            'POST',
            'foo/bar',
            ['Content-Type' => 'application/json'],
            '{"foo":"baz"}',
            true,
        );
        self::assertSame(['foo' => 'bar'], $response);
    }

    public function testSendPostRequestWithoutResponse(): void
    {
        $mockserver = new Mockserver();
        $mockserver->reset();
        $mockserver->expect([
            'httpRequest' => [
                'method' => 'DELETE',
                'path' => '/foo/bar',
                'headers' => [
                    'Authorization' => 'Bearer auth-token',
                ],
                'body' => ''
            ],
            'httpResponse' => [
                'statusCode' => 200,
            ],
        ]);

        $logger = new Logger('test');
        $authenticatorFactory = $this->createAuthenticatorFactory('auth-token');
        $guzzleClientFactory = new GuzzleClientFactory($logger);

        $apiClient = new AzureApiClient(
            $mockserver->getServerUrl(),
            'my-api',
            $guzzleClientFactory,
            $authenticatorFactory,
            $logger,
        );

        $response = $apiClient->sendRequest(
            'DELETE',
            'foo/bar',
            [],
            null,
            false,
        );
        self::assertNull($response);
    }

    public function createAuthenticatorFactory(string $authToken): AuthenticatorFactory
    {
        $authenticator = $this->createMock(AuthenticatorInterface::class);
        $authenticator->method('getAuthenticationToken')->willReturn($authToken);

        $authenticatorFactory = $this->createMock(AuthenticatorFactory::class);
        $authenticatorFactory->method('getAuthenticator')->willReturn($authenticator);

        return $authenticatorFactory;
    }
}
