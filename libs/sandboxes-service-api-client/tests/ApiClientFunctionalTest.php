<?php

declare(strict_types=1);

namespace Keboola\SandboxesServiceApiClient\Tests;

use GuzzleHttp\Psr7\Request;
use Keboola\SandboxesServiceApiClient\ApiClient;
use Keboola\SandboxesServiceApiClient\ApiClientConfiguration;
use Keboola\SandboxesServiceApiClient\Exception\ClientException;
use Keboola\SandboxesServiceApiClient\Json;
use PHPUnit\Framework\TestCase;

class ApiClientFunctionalTest extends TestCase
{
    private function getApiClient(string $url): ApiClient
    {
        return new ApiClient(new ApiClientConfiguration(
            $url,
            'token',
            'Keboola Sandboxes Service API PHP Client',
        ));
    }

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

        $apiClient = $this->getApiClient($mockserver->getServerUrl());
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
        self::assertSame('Keboola Sandboxes Service API PHP Client', $request['headers']['user-agent'] ?? null);
        self::assertArrayNotHasKey('content-type', $request['headers']);
    }

    public function testSendRequestWithDecodedResponse(): void
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

        $apiClient = $this->getApiClient($mockserver->getServerUrl());

        $response = $apiClient->sendRequestAndDecodeResponse(
            new Request(
                'POST',
                'foo/bar',
                ['Content-Type' => 'application/json'],
                '{"foo":"baz"}',
            ),
        );

        self::assertEquals(
            ['foo' => 'bar'],
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
        self::assertSame('Keboola Sandboxes Service API PHP Client', $request['headers']['user-agent'] ?? null);
        self::assertSame('application/json', $request['headers']['content-type'] ?? null);
        self::assertSame('{"foo":"baz"}', base64_decode($request['body']['rawBytes'] ?? ''));
    }

    public function testSendRequestWithArrayResponse(): void
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

        $apiClient = $this->getApiClient($mockserver->getServerUrl());

        $response = $apiClient->sendRequestAndDecodeResponse(
            new Request(
                'POST',
                'foo/bar',
                ['Content-Type' => 'application/json'],
                '{"foo":"baz"}',
            ),
        );

        self::assertEquals(
            [
                ['foo' => 'bar'],
                ['foo' => 'me'],
            ],
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
                    'error' => 'BadRequest',
                    'message' => 'This is not good',
                ]),
            ],
        ]);

        $apiClient = $this->getApiClient($mockserver->getServerUrl());

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

        $apiClient = $this->getApiClient($mockserver->getServerUrl());

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage(
            "GET http://mockserver:1080/foo/bar` resulted in a `400 Bad Request` response:\nGateway timeout",
        );

        $apiClient->sendRequest(new Request('GET', 'foo/bar'));
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

        $apiClient = $this->getApiClient($mockserver->getServerUrl());
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

        $apiClient = new ApiClient(new ApiClientConfiguration(
            $mockserver->getServerUrl(),
            storageToken: 'token',
            userAgent: 'Keboola Sandboxes Service API PHP Client',
            backoffMaxTries: 2,
        ));

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage(
            'Server error: `GET http://mockserver:1080/foo/bar` resulted in a `500 Internal Server Error` response:
error occurred',
        );

        $apiClient->sendRequest(new Request('GET', 'foo/bar'));
    }
}
