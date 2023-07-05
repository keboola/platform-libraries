<?php

declare(strict_types=1);

namespace Keboola\VaultApiClient\Tests;

use GuzzleHttp\Client as GuzzleClient;
use GuzzleHttp\Promise\Create;
use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Psr7\Response;
use GuzzleHttp\Psr7\Uri;
use InvalidArgumentException;
use Keboola\VaultApiClient\ApiClient;
use Keboola\VaultApiClient\ApiClientConfiguration;
use Keboola\VaultApiClient\Exception\ClientException;
use Keboola\VaultApiClient\Json;
use Monolog\Handler\TestHandler;
use Monolog\Logger;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;

class ApiClientTest extends TestCase
{
    use ApiClientTestTrait;
    use ReflectionPropertyAccessTestCase;

    private const BASE_URL = 'https://vault.keboola.com';
    private const API_TOKEN = 'my-token';

    private readonly LoggerInterface $logger;
    private readonly TestHandler $logsHandler;

    protected function setUp(): void
    {
        parent::setUp();

        $this->logsHandler = new TestHandler();
        $this->logger = new Logger('tests', [$this->logsHandler]);
    }

    public function testCreateClientWithDefaults(): void
    {
        $client = new ApiClient(self::BASE_URL, self::API_TOKEN);

        $httpClient = self::getPrivatePropertyValue($client, 'httpClient');
        self::assertInstanceOf(GuzzleClient::class, $httpClient);
        $httpClientConfig = self::getPrivatePropertyValue($httpClient, 'config');
        self::assertIsArray($httpClientConfig);

        self::assertEquals(new Uri(self::BASE_URL), $httpClientConfig['base_uri']);
        self::assertSame(['User-Agent' => 'Keboola Vault PHP Client'], $httpClientConfig['headers']);
        self::assertSame(120, $httpClientConfig['timeout']);
        self::assertSame(10, $httpClientConfig['connect_timeout']);
    }

    /**
     * @param non-empty-string $baseUrl
     * @param non-empty-string $token
     * @dataProvider provideInvalidOptions
     */
    public function testInvalidOptions(
        string $baseUrl,
        string $token,
        ?int $backoffMaxTries,
        string $expectedError
    ): void {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage($expectedError);

        new ApiClient(
            $baseUrl,
            $token,
            new ApiClientConfiguration(
                backoffMaxTries: $backoffMaxTries // @phpstan-ignore-line
            )
        );
    }

    public function provideInvalidOptions(): iterable
    {
        yield 'empty baseUrl' => [
            'baseUrl' => '',
            'token' => self::API_TOKEN,
            'backoffMaxTries' => 0,
            'error' => 'Base URL must be a non-empty string',
        ];

        yield 'empty token' => [
            'baseUrl' => self::BASE_URL,
            'token' => '',
            'backoffMaxTries' => 0,
            'error' => 'Token must be a non-empty string',
        ];

        yield 'negative backoffMaxTries' => [
            'baseUrl' => self::BASE_URL,
            'token' => self::API_TOKEN,
            'backoffMaxTries' => -1,
            'error' => 'Backoff max tries must be greater than or equal to 0',
        ];
    }

    public function testLogger(): void
    {

        $client = new ApiClient(
            self::BASE_URL,
            self::API_TOKEN,
            configuration: new ApiClientConfiguration(
                requestHandler: fn($request) => Create::promiseFor(new Response(201, [], 'boo')),
                logger: $this->logger,
            ),
        );
        $client->sendRequest(new Request('GET', '/'));
        self::assertTrue($this->logsHandler->hasInfoThatMatches(
            '#^[\w\d]+ Keboola Vault PHP Client - \[\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\+00:00\] "GET  /1.1" 201 $#',
        ));
    }

    public function testSendRequest(): void
    {
        $requestHandler = self::createRequestHandler($requestsHistory, [
            new Response(200),
        ]);

        $apiClient = new ApiClient(
            self::BASE_URL,
            self::API_TOKEN,
            new ApiClientConfiguration(
                requestHandler: $requestHandler(...)
            )
        );
        $apiClient->sendRequest(new Request('DELETE', 'foo/bar'));

        self::assertCount(1, $requestsHistory);
        $request = $requestsHistory[0]['request'];
        self::assertRequestEquals(
            'DELETE',
            self::BASE_URL . '/foo/bar',
            ['X-StorageApi-Token' => self::API_TOKEN],
            null,
            $request,
        );
    }

    public function testSendRequestAndMapResponse(): void
    {
        $requestHandler = self::createRequestHandler($requestsHistory, [
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                Json::encodeArray([
                    'foo' => 'bar',
                ]),
            ),
        ]);

        $apiClient = new ApiClient(
            self::BASE_URL,
            self::API_TOKEN,
            new ApiClientConfiguration(
                requestHandler: $requestHandler(...)
            )
        );
        $result = $apiClient->sendRequestAndMapResponse(
            new Request('GET', 'foo/bar'),
            DummyTestResponse::class,
        );

        self::assertCount(1, $requestsHistory);
        $request = $requestsHistory[0]['request'];
        self::assertRequestEquals(
            'GET',
            self::BASE_URL . '/foo/bar',
            ['X-StorageApi-Token' => self::API_TOKEN],
            null,
            $request,
        );

        self::assertEquals(DummyTestResponse::fromResponseData(['foo' => 'bar']), $result);
    }

    public function testSendRequestFailingWithClientError(): void
    {
        $requestHandler = self::createRequestHandler($requestsHistory, [
            new Response(
                400,
                ['Content-Type' => 'application/json'],
                Json::encodeArray([
                    'error' => 'Missing data',
                    'code' => 400,
                ]),
            ),
        ]);

        $apiClient = new ApiClient(
            self::BASE_URL,
            self::API_TOKEN,
            new ApiClientConfiguration(
                requestHandler: $requestHandler(...)
            )
        );

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('400: Missing data');

        $apiClient->sendRequest(new Request('DELETE', 'foo/bar'));
    }

    public function testSendRequestFailingWithNonStandardClientError(): void
    {
        $requestHandler = self::createRequestHandler($requestsHistory, [
            new Response(
                400,
                ['Content-Type' => 'application/json'],
                Json::encodeArray([
                    'error' => 'Missing data',
                ]),
            ),
        ]);

        $apiClient = new ApiClient(
            self::BASE_URL,
            self::API_TOKEN,
            new ApiClientConfiguration(
                requestHandler: $requestHandler(...)
            )
        );

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage(
            'Client error: `DELETE https://vault.keboola.com/foo/bar` resulted in a `400 Bad Request` response'
        );

        $apiClient->sendRequest(new Request('DELETE', 'foo/bar'));
    }

    public function testSendRequestFailingWithTransientServerError(): void
    {
        $requestHandler = self::createRequestHandler($requestsHistory, [
            new Response(
                500,
                [],
                'Internal Server Error',
            ),
            new Response(
                200,
            ),
        ]);

        $apiClient = new ApiClient(
            self::BASE_URL,
            self::API_TOKEN,
            new ApiClientConfiguration(
                requestHandler: $requestHandler(...)
            )
        );

        $apiClient->sendRequest(new Request('DELETE', 'foo/bar'));

        self::assertCount(2, $requestsHistory);
        self::assertRequestEquals(
            'DELETE',
            self::BASE_URL . '/foo/bar',
            ['X-StorageApi-Token' => self::API_TOKEN],
            null,
            $requestsHistory[0]['request'],
        );
        self::assertRequestEquals(
            'DELETE',
            self::BASE_URL . '/foo/bar',
            ['X-StorageApi-Token' => self::API_TOKEN],
            null,
            $requestsHistory[1]['request'],
        );
    }

    public function testSendRequestFailingWithPermanentServerError(): void
    {
        $requestHandler = self::createRequestHandler($requestsHistory, [
            new Response(
                500,
                [],
                'Internal Server Error',
            ),
            new Response(
                500,
                [],
                'Internal Server Error',
            ),
            new Response(
                500,
                [],
                'Internal Server Error',
            ),
        ]);

        $apiClient = new ApiClient(
            self::BASE_URL,
            self::API_TOKEN,
            new ApiClientConfiguration(
                backoffMaxTries: 2,
                requestHandler: $requestHandler(...)
            )
        );

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage(
            <<<EOF
            Server error: `DELETE https://vault.keboola.com/foo/bar` resulted in a `500 Internal Server Error` response:
            Internal Server Error
            EOF,
        );

        $apiClient->sendRequest(new Request('DELETE', 'foo/bar'));
    }

    public function testSendRequestWitServerErrorAndDisabledRetry(): void
    {
        $requestHandler = self::createRequestHandler($requestsHistory, [
            new Response(
                500,
                [],
                'Internal Server Error',
            ),
        ]);

        $apiClient = new ApiClient(
            self::BASE_URL,
            self::API_TOKEN,
            new ApiClientConfiguration(
                backoffMaxTries: 0,
                requestHandler: $requestHandler(...)
            )
        );

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage(
            <<<EOF
            Server error: `DELETE https://vault.keboola.com/foo/bar` resulted in a `500 Internal Server Error` response:
            Internal Server Error
            EOF,
        );

        $apiClient->sendRequest(new Request('DELETE', 'foo/bar'));
    }

    public function testSendRequestAndMapResponseFailingOnResponseMapping(): void
    {
        $requestHandler = self::createRequestHandler($requestsHistory, [
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                '{"foo":'
            ),
        ]);

        $apiClient = new ApiClient(
            self::BASE_URL,
            self::API_TOKEN,
            new ApiClientConfiguration(
                requestHandler: $requestHandler(...)
            )
        );

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('Response is not a valid JSON: Syntax error');

        $apiClient->sendRequestAndMapResponse(
            new Request('GET', 'foo/bar'),
            DummyTestResponse::class,
        );
    }
}
