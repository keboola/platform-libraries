<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Tests;

use GuzzleHttp\Middleware;
use GuzzleHttp\Psr7\Request;
use Keboola\AzureApiClient\ApiClient;
use Keboola\AzureApiClient\Exception\ClientException;
use Keboola\AzureApiClient\Json;
use Keboola\AzureApiClient\RetryDecider;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

class ApiClientFunctionalTest extends TestCase
{
    public function testSendRequest(): void
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
            ],
        ]);

        $apiClient = new ApiClient(
            baseUrl: $mockserver->getServerUrl(),
        );
        $apiClient->sendRequest(new Request('GET', 'foo/bar'));

        $recordedRequests = $mockserver->fetchRecordedRequests([
            'method' => 'GET',
            'path' => '/foo/bar',
        ]);
        self::assertCount(1, $recordedRequests);
        $request = $recordedRequests[0];

        self::assertSame('GET', $request['method']);
        self::assertSame('/foo/bar', $request['path']);
        self::assertTrue($request['keepAlive']);
        self::assertSame('Keboola Azure PHP Client', $request['headers']['user-agent'] ?? null);
        self::assertArrayNotHasKey('content-type', $request['headers']);
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
                    'Content-Type' => 'application/json',
                ],
                'body' => '{"foo":"baz"}',
            ],
            'httpResponse' => [
                'statusCode' => 200,
                'body' => Json::encodeArray(['foo' => 'bar']),
            ],
        ]);

        $apiClient = new ApiClient(
            baseUrl: $mockserver->getServerUrl(),
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

        $recordedRequests = $mockserver->fetchRecordedRequests([
            'method' => 'POST',
            'path' => '/foo/bar',
        ]);
        self::assertCount(1, $recordedRequests);
        $request = $recordedRequests[0];

        self::assertSame('POST', $request['method']);
        self::assertSame('/foo/bar', $request['path']);
        self::assertTrue($request['keepAlive']);
        self::assertSame('Keboola Azure PHP Client', $request['headers']['user-agent'] ?? null);
        self::assertSame('application/json', $request['headers']['content-type'] ?? null);
        self::assertSame('{"foo":"baz"}', base64_decode($request['body']['rawBytes'] ?? ''));
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
                    'Content-Type' => 'application/json',
                ],
                'body' => '{"foo":"baz"}',
            ],
            'httpResponse' => [
                'statusCode' => 200,
                'body' => Json::encodeArray([
                    ['foo' => 'bar'],
                    ['foo' => 'me'],
                ]),
            ],
        ]);

        $apiClient = new ApiClient(
            baseUrl: $mockserver->getServerUrl(),
        );

        $response = $apiClient->sendRequestAndMapResponse(
            new Request(
                'POST',
                'foo/bar',
                ['Content-Type' => 'application/json'],
                '{"foo":"baz"}',
            ),
            DummyTestResponse::class,
            [],
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
                'body' => Json::encodeArray([
                    'error' => [
                        'code' => 'BadRequest',
                        'message' => 'This is not good',
                    ],
                ]),
            ],
        ]);

        $apiClient = new ApiClient(
            baseUrl: $mockserver->getServerUrl(),
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

        $apiClient = new ApiClient(
            baseUrl: $mockserver->getServerUrl(),
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

        $apiClient = new ApiClient(
            baseUrl: $mockserver->getServerUrl(),
        );

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage(
            'Failed to map response data: Keboola\AzureApiClient\Tests\DummyTestResponse::__construct(): ' .
            'Argument #1 ($foo) must be of type string, null given, called in ' .
            '/code/libs/azure-api-client/tests/DummyTestResponse.php on line'
        );

        $apiClient->sendRequestAndMapResponse(new Request('GET', 'foo/bar'), DummyTestResponse::class);
    }

    public function testRetrySuccess(): void
    {
        $mockserver = new Mockserver();
        $mockserver->reset();
        $mockserver->expect([
            'httpRequest' => [
                'method' => 'GET',
                'path' => '/foo/bar',
            ],
            'httpResponse' => [
                'statusCode' => 500,
            ],
            'times' => [
                'remainingTimes' => 2,
            ],
        ]);
        $mockserver->expect([
            'httpRequest' => [
                'method' => 'GET',
                'path' => '/foo/bar',
            ],
            'httpResponse' => [
                'statusCode' => 200,
            ],
        ]);

        $apiClient = new ApiClient(
            baseUrl: $mockserver->getServerUrl(),
            retryMiddleware: Middleware::retry(
                new RetryDecider(2, new NullLogger())
            )
        );
        $apiClient->sendRequest(new Request('GET', 'foo/bar'));

        $recordedRequests = $mockserver->fetchRecordedRequests([
            'method' => 'GET',
            'path' => '/foo/bar',
        ]);
        self::assertCount(3, $recordedRequests);
    }

    public function testRetryFailure(): void
    {
        $mockserver = new Mockserver();
        $mockserver->reset();
        $mockserver->expect([
            'httpRequest' => [
                'method' => 'GET',
                'path' => '/foo/bar',
            ],
            'httpResponse' => [
                'statusCode' => 500,
                'body' => 'error occurred',
            ],
        ]);

        $apiClient = new ApiClient(
            baseUrl: $mockserver->getServerUrl(),
            retryMiddleware: Middleware::retry(
                new RetryDecider(2, new NullLogger())
            )
        );

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage(
            'Server error: `GET http://mockserver:1080/foo/bar` resulted in a `500 Internal Server Error` response:
error occurred'
        );

        $apiClient->sendRequest(new Request('GET', 'foo/bar'));
    }

    public function testRetryOnThrottling(): void
    {
        $mockserver = new Mockserver();
        $mockserver->reset();
        $mockserver->expect([
            'httpRequest' => [
                'method' => 'GET',
                'path' => '/foo/bar',
            ],
            'httpResponse' => [
                'statusCode' => 429,
                'body' => 'too many requests',
            ],
            'times' => [
                'remainingTimes' => 2,
            ],
        ]);
        $mockserver->expect([
            'httpRequest' => [
                'method' => 'GET',
                'path' => '/foo/bar',
            ],
            'httpResponse' => [
                'statusCode' => 200,
                'body' => '{"foo":"bar"}',
            ],
        ]);

        $apiClient = new ApiClient(
            baseUrl: $mockserver->getServerUrl(),
            retryMiddleware: Middleware::retry(
                new RetryDecider(2, new NullLogger())
            )
        );

        $response = $apiClient->sendRequestAndMapResponse(
            new Request('GET', 'foo/bar'),
            DummyTestResponse::class,
        );

        self::assertEquals(DummyTestResponse::fromResponseData(['foo' => 'bar']), $response);
    }
}
