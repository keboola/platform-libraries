<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Tests;

use GuzzleHttp\Psr7\Request;
use Keboola\AzureApiClient\Authentication\AuthenticatorFactory;
use Keboola\AzureApiClient\Authentication\AuthenticatorInterface;
use Keboola\AzureApiClient\AzureApiClient;
use Keboola\AzureApiClient\Exception\ClientException;
use Keboola\AzureApiClient\GuzzleClientFactory;
use Monolog\Logger;
use PHPUnit\Framework\TestCase;

class AzureApiClientFunctionalTest extends TestCase
{
    public function testSendRequest(): void
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

        $apiClient->sendRequest(new Request('GET', 'foo/bar'));

        self::assertTrue($mockserver->hasRecordedRequest([
            'method' => 'GET',
            'path' => '/foo/bar',
            'headers' => [
                'Authorization' => 'Bearer auth-token',
            ],
        ]));
    }

    public function testSendRequestWithMappedResponse(): void
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
                'body' => '{"foo":"baz"}',
            ],
            'httpResponse' => [
                'statusCode' => 200,
                'body' => (string) json_encode(['foo' => 'bar']),
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

        $response = $apiClient->sendRequestAndMapResponse(
            new Request(
                'POST',
                'foo/bar',
                ['Content-Type' => 'application/json'],
                '{"foo":"baz"}',
            ),
            DummyTestResponse::class,
        );

        self::assertEquals(
            new DummyTestResponse('bar'),
            $response,
        );
    }

    public function testSendRequestWithMappedArrayResponse(): void
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
                'body' => '{"foo":"baz"}',
            ],
            'httpResponse' => [
                'statusCode' => 200,
                'body' => (string) json_encode([
                    ['foo' => 'bar'],
                    ['foo' => 'me'],
                ]),
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

        $response = $apiClient->sendRequestAndMapResponse(
            new Request(
                'POST',
                'foo/bar',
                ['Content-Type' => 'application/json'],
                '{"foo":"baz"}',
            ),
            DummyTestResponse::class,
            true,
        );

        self::assertEquals(
            [new DummyTestResponse('bar'), new DummyTestResponse('me')],
            $response,
        );
    }

    public function testSendRequestFailingWithRegularError(): void
    {
        $mockserver = new Mockserver();
        $mockserver->reset();
        $mockserver->expect([
            'httpRequest' => [
                'method' => 'GET',
                'path' => '/foo/bar',
            ],
            'httpResponse' => [
                'statusCode' => 400,
                'body' => (string) json_encode([
                    'error' => [
                        'code' => 'BadRequest',
                        'message' => 'This is not good',
                    ],
                ]),
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

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('BadRequest: This is not good');

        $apiClient->sendRequest(new Request('GET', 'foo/bar'));
    }

    public function testSendRequestFailingWithUnexpectedError(): void
    {
        $mockserver = new Mockserver();
        $mockserver->reset();
        $mockserver->expect([
            'httpRequest' => [
                'method' => 'GET',
                'path' => '/foo/bar',
            ],
            'httpResponse' => [
                'statusCode' => 400,
                'body' => 'Gateway timeout',
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

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage(
            "GET http://mockserver:1080/foo/bar` resulted in a `400 Bad Request` response:\nGateway timeout"
        );

        $apiClient->sendRequest(new Request('GET', 'foo/bar'));
    }

    public function testSendRequestFailingOnResponseMapping(): void
    {
        $mockserver = new Mockserver();
        $mockserver->reset();
        $mockserver->expect([
            'httpRequest' => [
                'method' => 'GET',
                'path' => '/foo/bar',
            ],
            'httpResponse' => [
                'statusCode' => 200,
                'body' => '{"foo": null}',
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

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage(
            'Failed to map response data: Keboola\AzureApiClient\Tests\DummyTestResponse::__construct(): ' .
            'Argument #1 ($foo) must be of type string, null given, called in ' .
            '/code/libs/azure-api-client/tests/DummyTestResponse.php on line'
        );

        $apiClient->sendRequestAndMapResponse(new Request('GET', 'foo/bar'), DummyTestResponse::class);
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
