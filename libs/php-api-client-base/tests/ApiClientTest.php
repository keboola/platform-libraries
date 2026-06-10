<?php

declare(strict_types=1);

namespace Keboola\ApiClientBase\Tests;

use Keboola\ApiClientBase\ApiClient;
use Keboola\ApiClientBase\ApiClientOptions;
use Keboola\ApiClientBase\Auth\ManageApiTokenAuthenticator;
use Keboola\ApiClientBase\Auth\NoAuthAuthenticator;
use Keboola\ApiClientBase\Auth\RequestAuthenticatorInterface;
use Keboola\ApiClientBase\ErrorMessageResolverInterface;
use Keboola\ApiClientBase\Exception\ClientException;
use Keboola\ApiClientBase\Tests\Fixtures\DummyModel;
use PHPUnit\Framework\TestCase;
use Symfony\Component\HttpClient\MockHttpClient;
use Symfony\Component\HttpClient\Response\MockResponse;

class ApiClientTest extends TestCase
{
    /**
     * @param list<MockResponse> $responses
     */
    private function mockClient(array $responses): MockHttpClient
    {
        return new MockHttpClient($responses, 'https://example.test/');
    }

    private function apiClient(MockHttpClient $mock, ApiClientOptions $options): ApiClient
    {
        return new ApiClient('https://example.test', new NoAuthAuthenticator(), $options);
    }

    /**
     * Header lines in MockResponse::getRequestOptions() are `Name: value`; normalize them
     * to a value map keyed by lowercased header name for assertions.
     *
     * @return array<string, string>
     */
    private function requestHeaders(MockResponse $response): array
    {
        $headers = [];
        /** @var list<string> $lines */
        $lines = $response->getRequestOptions()['headers'] ?? [];
        foreach ($lines as $line) {
            [$name, $value] = explode(': ', $line, 2);
            $headers[strtolower($name)] = $value;
        }
        return $headers;
    }

    public function testSendsWithoutAuthHeaderWhenNoAuthAuthenticator(): void
    {
        $response = new MockResponse('{}');
        $mock = $this->mockClient([$response]);
        $client = $this->apiClient($mock, new ApiClientOptions(httpClient: $mock));
        $client->sendRequest('GET', 'foo');

        // The NoAuthAuthenticator contributes no headers, so none of the Keboola auth headers
        // are present on the wire.
        $headers = $this->requestHeaders($response);
        self::assertArrayNotHasKey('x-kbc-manageapitoken', $headers);
        self::assertArrayNotHasKey('x-storageapi-token', $headers);
        self::assertArrayNotHasKey('x-kubernetes-authorization', $headers);
    }

    public function testAddsAuthHeaderPerRequest(): void
    {
        $response = new MockResponse('{}');
        $mock = $this->mockClient([$response]);
        $client = new ApiClient(
            'https://example.test',
            new ManageApiTokenAuthenticator('secret-token'),
            new ApiClientOptions(httpClient: $mock),
        );
        $client->sendRequest('GET', 'foo');

        self::assertSame('secret-token', $this->requestHeaders($response)['x-kbc-manageapitoken'] ?? null);
    }

    public function testMapsResponseToModel(): void
    {
        $mock = $this->mockClient([new MockResponse('{"name":"foo"}')]);
        $client = $this->apiClient($mock, new ApiClientOptions(httpClient: $mock));
        $model = $client->sendRequestAndMapResponse('GET', 'foo', DummyModel::class);

        self::assertInstanceOf(DummyModel::class, $model);
        self::assertSame('foo', $model->name);
    }

    public function testMapsResponseToList(): void
    {
        $mock = $this->mockClient([new MockResponse('[{"name":"a"},{"name":"b"}]')]);
        $client = $this->apiClient($mock, new ApiClientOptions(httpClient: $mock));
        $models = $client->sendRequestAndMapResponse('GET', 'foo', DummyModel::class, [], true);

        self::assertCount(2, $models);
        self::assertSame('a', $models[0]->name);
        self::assertSame('b', $models[1]->name);
    }

    public function testSendsJsonBody(): void
    {
        $response = new MockResponse('{}');
        $mock = $this->mockClient([$response]);
        $client = $this->apiClient($mock, new ApiClientOptions(httpClient: $mock));
        $client->sendRequest('POST', 'foo', ['json' => ['name' => 'app-1']]);

        self::assertSame('POST', $response->getRequestMethod());
        self::assertSame('https://example.test/foo', $response->getRequestUrl());
        self::assertSame('{"name":"app-1"}', $response->getRequestOptions()['body'] ?? null);
    }

    public function testRetriesOn5xxThenSucceeds(): void
    {
        $mock = $this->mockClient([new MockResponse('', ['http_code' => 500]), new MockResponse('{}')]);
        $client = $this->apiClient($mock, new ApiClientOptions(httpClient: $mock));
        $client->sendRequest('GET', 'foo');

        self::assertSame(2, $mock->getRequestsCount());
    }

    public function testRetriesConfiguredStatusCode(): void
    {
        $mock = $this->mockClient([new MockResponse('', ['http_code' => 429]), new MockResponse('{}')]);
        $client = new ApiClient(
            'https://example.test',
            new NoAuthAuthenticator(),
            new ApiClientOptions(backoffMaxTries: 2, httpClient: $mock),
            retryableStatusCodes: [429],
        );
        $client->sendRequest('GET', 'foo');

        self::assertSame(2, $mock->getRequestsCount());
    }

    public function testThrowsClientExceptionWithDefaultMessageExtraction(): void
    {
        $mock = $this->mockClient([new MockResponse('{"error":"bad input"}', ['http_code' => 400])]);
        $client = $this->apiClient($mock, new ApiClientOptions(httpClient: $mock));

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('bad input');
        $this->expectExceptionCode(400);
        $client->sendRequest('GET', 'foo');
    }

    public function testUsesCustomErrorMessageResolver(): void
    {
        $mock = $this->mockClient([
            new MockResponse('{"code":"CONFLICT","error":"already exists"}', ['http_code' => 409]),
        ]);
        $resolver = new class implements ErrorMessageResolverInterface {
            public function __invoke(string $responseBody, int $statusCode): ?string
            {
                /** @var array{code?: string, error?: string} $data */
                $data = json_decode($responseBody, true) ?? [];
                return ($data['code'] ?? '') . ': ' . ($data['error'] ?? '');
            }
        };
        $client = new ApiClient(
            'https://example.test',
            new NoAuthAuthenticator(),
            new ApiClientOptions(httpClient: $mock),
            errorMessageResolver: $resolver,
        );

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('CONFLICT: already exists');
        $client->sendRequest('GET', 'foo');
    }

    public function testReExecutesAuthenticatorOnEachRetryAttempt(): void
    {
        $authenticator = new class implements RequestAuthenticatorInterface {
            public int $calls = 0;

            public function getAuthenticationHeaders(): array
            {
                $this->calls++;
                return ['X-Attempt' => 'call-' . $this->calls];
            }
        };

        $finalResponse = new MockResponse('{}');
        $mock = $this->mockClient([new MockResponse('', ['http_code' => 500]), $finalResponse]);
        $client = new ApiClient(
            'https://example.test',
            $authenticator,
            new ApiClientOptions(backoffMaxTries: 2, httpClient: $mock),
        );
        $client->sendRequest('GET', 'foo');

        self::assertSame(2, $authenticator->calls);
        self::assertSame('call-2', $this->requestHeaders($finalResponse)['x-attempt'] ?? null);
    }

    public function testThrowsClientExceptionAfterRetriesExhausted(): void
    {
        $mock = $this->mockClient([
            new MockResponse('', ['http_code' => 500]),
            new MockResponse('', ['http_code' => 500]),
        ]);
        $client = $this->apiClient($mock, new ApiClientOptions(backoffMaxTries: 1, httpClient: $mock));

        $this->expectException(ClientException::class);
        $this->expectExceptionCode(500);
        $client->sendRequest('GET', 'foo');
    }
}
