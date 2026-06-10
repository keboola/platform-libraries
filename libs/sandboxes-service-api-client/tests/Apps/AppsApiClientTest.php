<?php

declare(strict_types=1);

namespace Keboola\SandboxesServiceApiClient\Tests\Apps;

use GuzzleHttp\Client as GuzzleClient;
use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\Psr7\Response;
use InvalidArgumentException;
use Keboola\ApiClientBase\ApiClient;
use Keboola\ApiClientBase\Exception\ClientException;
use Keboola\ApiClientBase\Json;
use Keboola\SandboxesServiceApiClient\Apps\App;
use Keboola\SandboxesServiceApiClient\Apps\AppsApiClient;
use Keboola\SandboxesServiceApiClient\Tests\ReflectionPropertyAccessTestCase;
use Monolog\Handler\TestHandler;
use Monolog\Logger;
use PHPUnit\Framework\TestCase;
use Psr\Http\Message\RequestInterface;
use stdClass;

class AppsApiClientTest extends TestCase
{
    use ReflectionPropertyAccessTestCase;

    public function testListApps(): void
    {
        $responseBody = [
            [
                'id' => 'app-id-1',
                'projectId' => 'project-id',
                'componentId' => 'keboola.data-apps',
                'branchId' => null,
                'configId' => 'config-id-1',
                'configVersion' => '1',
                'state' => 'running',
                'desiredState' => 'running',
                'lastRequestTimestamp' => '2024-02-01T08:00:00+01:00',
                'url' => 'https://example.com',
                'autoSuspendAfterSeconds' => 3600,
                'provisioningStrategy' => 'operator',
            ],
            [
                'id' => 'app-id-2',
                'projectId' => 'project-id',
                'componentId' => 'keboola.data-apps',
                'branchId' => 'branch-id',
                'configId' => 'config-id-2',
                'configVersion' => '2',
                'state' => 'stopped',
                'desiredState' => 'stopped',
                'lastRequestTimestamp' => null,
                'url' => null,
                'autoSuspendAfterSeconds' => 0,
                'provisioningStrategy' => 'jobQueue',
            ],
        ];

        $mock = new MockHandler([
            new Response(200, ['Content-Type' => 'application/json'], Json::encodeArray($responseBody)),
        ]);
        [$requestHandler, $store] = self::createCapturingHandler($mock);

        $client = new AppsApiClient(
            'https://data-apps.keboola.com',
            'my-token',
            backoffMaxTries: 0,
            requestHandler: $requestHandler,
        );
        $result = $client->listApps();

        $expectedApps = [
            App::fromResponseData($responseBody[0]),
            App::fromResponseData($responseBody[1]),
        ];
        self::assertEquals($expectedApps, $result);

        self::assertCount(1, $store->requests);
        self::assertRequestEquals(
            'GET',
            'https://data-apps.keboola.com/apps',
            [
                'X-StorageApi-Token' => 'my-token',
            ],
            '',
            $store->requests[0],
        );
    }

    public function testListAppsWithOffsetAndLimit(): void
    {
        $responseBody = [
            [
                'id' => 'app-id-1',
                'projectId' => 'project-id',
                'componentId' => 'keboola.data-apps',
                'branchId' => null,
                'configId' => 'config-id-1',
                'configVersion' => '1',
                'state' => 'running',
                'desiredState' => 'running',
                'lastRequestTimestamp' => '2024-02-01T08:00:00+01:00',
                'url' => 'https://example.com',
                'autoSuspendAfterSeconds' => 3600,
                'provisioningStrategy' => 'operator',
            ],
        ];

        $mock = new MockHandler([
            new Response(200, ['Content-Type' => 'application/json'], Json::encodeArray($responseBody)),
        ]);
        [$requestHandler, $store] = self::createCapturingHandler($mock);

        $client = new AppsApiClient(
            'https://data-apps.keboola.com',
            'my-token',
            backoffMaxTries: 0,
            requestHandler: $requestHandler,
        );
        $result = $client->listApps(10, 50);

        $expectedApps = [App::fromResponseData($responseBody[0])];
        self::assertEquals($expectedApps, $result);

        self::assertCount(1, $store->requests);
        self::assertRequestEquals(
            'GET',
            'https://data-apps.keboola.com/apps?offset=10&limit=50',
            [
                'X-StorageApi-Token' => 'my-token',
            ],
            '',
            $store->requests[0],
        );
    }

    public function testGetApp(): void
    {
        $responseBody = [
            'id' => 'app-id',
            'projectId' => 'project-id',
            'componentId' => 'keboola.data-apps',
            'branchId' => null,
            'configId' => 'config-id',
            'configVersion' => '5',
            'state' => 'running',
            'desiredState' => 'running',
            'lastRequestTimestamp' => '2024-02-01T08:00:00+01:00',
            'url' => 'https://example.com',
            'autoSuspendAfterSeconds' => 3600,
            'provisioningStrategy' => 'operator',
        ];

        $mock = new MockHandler([
            new Response(200, ['Content-Type' => 'application/json'], Json::encodeArray($responseBody)),
        ]);
        [$requestHandler, $store] = self::createCapturingHandler($mock);

        $client = new AppsApiClient(
            'https://data-apps.keboola.com',
            'my-token',
            backoffMaxTries: 0,
            requestHandler: $requestHandler,
        );
        $result = $client->getApp('app-id');

        self::assertEquals(App::fromResponseData($responseBody), $result);

        self::assertCount(1, $store->requests);
        self::assertRequestEquals(
            'GET',
            'https://data-apps.keboola.com/apps/app-id',
            [
                'X-StorageApi-Token' => 'my-token',
            ],
            '',
            $store->requests[0],
        );
    }

    public function testPatchApp(): void
    {
        $mock = new MockHandler([new Response(200)]);
        [$requestHandler, $store] = self::createCapturingHandler($mock);

        $client = new AppsApiClient(
            'https://data-apps.keboola.com',
            'my-token',
            backoffMaxTries: 0,
            requestHandler: $requestHandler,
        );
        $client->patchApp('app-id', [
            'desiredState' => 'stopped',
            'restartIfRunning' => false,
        ]);

        self::assertCount(1, $store->requests);
        self::assertRequestEquals(
            'PATCH',
            'https://data-apps.keboola.com/apps/app-id',
            [
                'X-StorageApi-Token' => 'my-token',
                'Content-Type' => 'application/json',
            ],
            Json::encodeArray([
                'desiredState' => 'stopped',
                'restartIfRunning' => false,
            ]),
            $store->requests[0],
        );
    }

    public function testListAppsWithOnlyOffset(): void
    {
        $mock = new MockHandler([
            new Response(200, ['Content-Type' => 'application/json'], Json::encodeArray([])),
        ]);
        [$requestHandler, $store] = self::createCapturingHandler($mock);

        $client = new AppsApiClient(
            'https://data-apps.keboola.com',
            'my-token',
            backoffMaxTries: 0,
            requestHandler: $requestHandler,
        );
        $result = $client->listApps(10);

        self::assertEquals([], $result);

        self::assertCount(1, $store->requests);
        self::assertRequestEquals(
            'GET',
            'https://data-apps.keboola.com/apps?offset=10',
            [
                'X-StorageApi-Token' => 'my-token',
            ],
            '',
            $store->requests[0],
        );
    }

    public function testListAppsWithOnlyLimit(): void
    {
        $mock = new MockHandler([
            new Response(200, ['Content-Type' => 'application/json'], Json::encodeArray([])),
        ]);
        [$requestHandler, $store] = self::createCapturingHandler($mock);

        $client = new AppsApiClient(
            'https://data-apps.keboola.com',
            'my-token',
            backoffMaxTries: 0,
            requestHandler: $requestHandler,
        );
        $result = $client->listApps(null, 50);

        self::assertEquals([], $result);

        self::assertCount(1, $store->requests);
        self::assertRequestEquals(
            'GET',
            'https://data-apps.keboola.com/apps?limit=50',
            [
                'X-StorageApi-Token' => 'my-token',
            ],
            '',
            $store->requests[0],
        );
    }

    public function testListAppsWithTypes(): void
    {
        $responseBody = [
            [
                'id' => 'app-id-1',
                'projectId' => 'project-id',
                'componentId' => 'keboola.jupyter-sandbox',
                'type' => 'python',
                'branchId' => null,
                'configId' => 'config-id-1',
                'configVersion' => '1',
                'state' => 'running',
                'desiredState' => 'running',
                'lastRequestTimestamp' => '2024-02-01T08:00:00+01:00',
                'url' => null,
                'autoSuspendAfterSeconds' => 3600,
                'provisioningStrategy' => 'operator',
            ],
        ];

        $mock = new MockHandler([
            new Response(200, ['Content-Type' => 'application/json'], Json::encodeArray($responseBody)),
        ]);
        [$requestHandler, $store] = self::createCapturingHandler($mock);

        $client = new AppsApiClient(
            'https://data-apps.keboola.com',
            'my-token',
            backoffMaxTries: 0,
            requestHandler: $requestHandler,
        );
        $result = $client->listApps(types: ['python', 'r']);

        self::assertEquals([App::fromResponseData($responseBody[0])], $result);

        self::assertCount(1, $store->requests);
        self::assertRequestEquals(
            'GET',
            'https://data-apps.keboola.com/apps?type%5B0%5D=python&type%5B1%5D=r',
            [
                'X-StorageApi-Token' => 'my-token',
            ],
            '',
            $store->requests[0],
        );
    }

    public function testCreateApp(): void
    {
        $responseBody = [
            'id' => 'new-app-id',
            'projectId' => 'project-id',
            'componentId' => 'keboola.data-apps',
            'branchId' => '123',
            'configId' => 'config-id',
            'configVersion' => '1',
            'state' => 'created',
            'desiredState' => 'running',
            'lastRequestTimestamp' => null,
            'url' => null,
            'autoSuspendAfterSeconds' => 3600,
            'provisioningStrategy' => 'operator',
        ];

        $mock = new MockHandler([
            new Response(200, ['Content-Type' => 'application/json'], Json::encodeArray($responseBody)),
        ]);
        [$requestHandler, $store] = self::createCapturingHandler($mock);

        $client = new AppsApiClient(
            'https://data-apps.keboola.com',
            'my-token',
            backoffMaxTries: 0,
            requestHandler: $requestHandler,
        );

        $payload = [
            'type' => 'streamlit',
            'branchId' => '123',
            'name' => 'Test App',
            'description' => 'Test description',
            'config' => ['key' => 'value'],
        ];
        $result = $client->createApp($payload);

        self::assertEquals(App::fromResponseData($responseBody), $result);

        self::assertCount(1, $store->requests);
        self::assertRequestEquals(
            'POST',
            'https://data-apps.keboola.com/apps',
            [
                'X-StorageApi-Token' => 'my-token',
                'Content-Type' => 'application/json',
            ],
            Json::encodeArray($payload),
            $store->requests[0],
        );
    }

    public function testDeleteApp(): void
    {
        $mock = new MockHandler([new Response(202)]);
        [$requestHandler, $store] = self::createCapturingHandler($mock);

        $client = new AppsApiClient(
            'https://data-apps.keboola.com',
            'my-token',
            backoffMaxTries: 0,
            requestHandler: $requestHandler,
        );
        $client->deleteApp('app-id');

        self::assertCount(1, $store->requests);
        self::assertRequestEquals(
            'DELETE',
            'https://data-apps.keboola.com/apps/app-id',
            [
                'X-StorageApi-Token' => 'my-token',
            ],
            '',
            $store->requests[0],
        );
    }

    /**
     * @param MockHandler $mock
     * @return array{0: \Closure, 1: stdClass}
     */
    private static function createCapturingHandler(MockHandler $mock): array
    {
        $store = new stdClass();
        $store->requests = [];
        $handler = static function (RequestInterface $request, array $options) use ($mock, $store) {
            $store->requests[] = $request;
            return $mock($request, $options);
        };

        return [$handler, $store];
    }

    private static function assertRequestEquals(
        string $method,
        string $uri,
        array $headers,
        ?string $body,
        RequestInterface $request,
    ): void {
        self::assertSame($method, $request->getMethod());
        self::assertSame($uri, $request->getUri()->__toString());

        foreach ($headers as $headerName => $headerValue) {
            self::assertSame($headerValue, $request->getHeaderLine($headerName));
        }

        self::assertSame($body ?? '', $request->getBody()->getContents());
    }

    public function testCustomUserAgentIsPassedInRequest(): void
    {
        $mock = new MockHandler([
            new Response(200, ['Content-Type' => 'application/json'], Json::encodeArray([])),
        ]);
        [$requestHandler, $store] = self::createCapturingHandler($mock);

        $client = new AppsApiClient(
            'https://data-apps.keboola.com',
            'my-token',
            backoffMaxTries: 0,
            userAgent: 'Custom Agent/1.0',
            requestHandler: $requestHandler,
        );
        $client->listApps();

        self::assertCount(1, $store->requests);
        self::assertSame(
            'Custom Agent/1.0',
            $store->requests[0]->getHeaderLine('User-Agent'),
        );
    }

    public function testDefaultOptionsWithRetry(): void
    {
        // Test that default backoffMaxTries=5 allows retries on 500 errors
        $mock = new MockHandler([
            new Response(500),
            new Response(200, ['Content-Type' => 'application/json'], Json::encodeArray([])),
        ]);
        [$requestHandler, $store] = self::createCapturingHandler($mock);

        $client = new AppsApiClient(
            'https://data-apps.keboola.com',
            'my-token',
            requestHandler: $requestHandler,
        );
        // Should succeed after retry
        $result = $client->listApps();
        self::assertSame([], $result);
        self::assertCount(2, $store->requests);
    }

    public function testConstructWithNullOptionsDoesNotThrow(): void
    {
        // With null options the facade should construct without error
        $client = new AppsApiClient(
            'https://data-apps.keboola.com',
            'my-token',
        );
        self::assertInstanceOf(AppsApiClient::class, $client);
    }

    public function testEmptyTokenThrows(): void
    {
        $this->expectException(InvalidArgumentException::class);
        // @phpstan-ignore argument.type
        new AppsApiClient('https://data-apps.keboola.com', '');
    }

    public function testErrorMessageResolverCombinesErrorAndMessage(): void
    {
        // Test custom error resolver: error+message format "BadRequest: This is not good"
        $mock = new MockHandler([
            new Response(
                400,
                ['Content-Type' => 'application/json'],
                Json::encodeArray(['error' => 'BadRequest', 'message' => 'This is not good']),
            ),
        ]);
        [$requestHandler, $store] = self::createCapturingHandler($mock);

        $client = new AppsApiClient(
            'https://data-apps.keboola.com',
            'my-token',
            backoffMaxTries: 0,
            requestHandler: $requestHandler,
        );

        try {
            $client->getApp('app-id');
            self::fail('Expected ClientException');
        } catch (ClientException $e) {
            self::assertSame('BadRequest: This is not good', $e->getMessage());
            self::assertCount(1, $store->requests);
        }
    }

    public function testErrorMessageResolverTrimsResult(): void
    {
        // UnwrapTrim: verify that trim() removes trailing whitespace from the combined message
        $mock = new MockHandler([
            new Response(
                400,
                ['Content-Type' => 'application/json'],
                Json::encodeArray(['error' => 'BadRequest', 'message' => 'This is not good ']),
            ),
        ]);
        [$requestHandler] = self::createCapturingHandler($mock);

        $client = new AppsApiClient(
            'https://data-apps.keboola.com',
            'my-token',
            backoffMaxTries: 0,
            requestHandler: $requestHandler,
        );

        try {
            $client->getApp('app-id');
            self::fail('Expected ClientException');
        } catch (ClientException $e) {
            // trim() should remove trailing space → 'BadRequest: This is not good' not 'BadRequest: This is not good '
            self::assertSame('BadRequest: This is not good', $e->getMessage());
        }
    }

    public function testErrorMessageResolverDoesNotCombineWhenErrorMissing(): void
    {
        // LogicalAnd: verify that missing 'error' key prevents custom format
        $mock = new MockHandler([
            new Response(
                400,
                ['Content-Type' => 'application/json'],
                Json::encodeArray(['message' => 'This is not good']),
            ),
        ]);
        [$requestHandler] = self::createCapturingHandler($mock);

        $client = new AppsApiClient(
            'https://data-apps.keboola.com',
            'my-token',
            backoffMaxTries: 0,
            requestHandler: $requestHandler,
        );

        try {
            $client->getApp('app-id');
            self::fail('Expected ClientException');
        } catch (ClientException $e) {
            // Should NOT be in "error: message" format since 'error' key is missing
            self::assertStringNotContainsString(': This is not good', $e->getMessage());
        }
    }

    public function testErrorMessageResolverDoesNotCombineWhenMessageMissing(): void
    {
        // LogicalAnd: verify that missing 'message' key prevents custom format
        $mock = new MockHandler([
            new Response(
                400,
                ['Content-Type' => 'application/json'],
                Json::encodeArray(['error' => 'BadRequest']),
            ),
        ]);
        [$requestHandler] = self::createCapturingHandler($mock);

        $client = new AppsApiClient(
            'https://data-apps.keboola.com',
            'my-token',
            backoffMaxTries: 0,
            requestHandler: $requestHandler,
        );

        try {
            $client->getApp('app-id');
            self::fail('Expected ClientException');
        } catch (ClientException $e) {
            // Should NOT produce "BadRequest: " format since 'message' key is missing
            self::assertStringNotContainsString('BadRequest: ', $e->getMessage());
        }
    }

    public function testErrorMessageResolverDoesNotCombineWhenErrorIsEmpty(): void
    {
        // LogicalAnd: verify that empty 'error' string prevents custom format
        $mock = new MockHandler([
            new Response(
                400,
                ['Content-Type' => 'application/json'],
                Json::encodeArray(['error' => '', 'message' => 'This is not good']),
            ),
        ]);
        [$requestHandler] = self::createCapturingHandler($mock);

        $client = new AppsApiClient(
            'https://data-apps.keboola.com',
            'my-token',
            backoffMaxTries: 0,
            requestHandler: $requestHandler,
        );

        try {
            $client->getApp('app-id');
            self::fail('Expected ClientException');
        } catch (ClientException $e) {
            // Empty error field → should NOT produce ": This is not good" format
            self::assertStringNotContainsString(': This is not good', $e->getMessage());
        }
    }

    public function testErrorMessageResolverDoesNotCombineWhenMessageIsEmpty(): void
    {
        // LogicalAnd: verify that empty 'message' string prevents custom format
        $mock = new MockHandler([
            new Response(
                400,
                ['Content-Type' => 'application/json'],
                Json::encodeArray(['error' => 'BadRequest', 'message' => '']),
            ),
        ]);
        [$requestHandler] = self::createCapturingHandler($mock);

        $client = new AppsApiClient(
            'https://data-apps.keboola.com',
            'my-token',
            backoffMaxTries: 0,
            requestHandler: $requestHandler,
        );

        try {
            $client->getApp('app-id');
            self::fail('Expected ClientException');
        } catch (ClientException $e) {
            // Empty message field → should NOT produce "BadRequest: " format
            self::assertStringNotContainsString('BadRequest: ', $e->getMessage());
        }
    }

    private static function getHttpClient(AppsApiClient $client): GuzzleClient
    {
        $apiClient = self::getPrivatePropertyValue($client, 'apiClient');
        self::assertInstanceOf(ApiClient::class, $apiClient);
        $httpClient = self::getPrivatePropertyValue($apiClient, 'httpClient');
        self::assertInstanceOf(GuzzleClient::class, $httpClient);
        return $httpClient;
    }

    public function testDefaultConnectTimeoutIs10(): void
    {
        $client = new AppsApiClient('https://data-apps.keboola.com', 'my-token');
        self::assertSame(10, self::getHttpClient($client)->getConfig('connect_timeout'));
    }

    public function testDefaultRequestTimeoutIs120(): void
    {
        $client = new AppsApiClient('https://data-apps.keboola.com', 'my-token');
        self::assertSame(120, self::getHttpClient($client)->getConfig('timeout'));
    }

    public function testCustomConnectTimeoutIsUsed(): void
    {
        $client = new AppsApiClient('https://data-apps.keboola.com', 'my-token', connectTimeout: 30);
        self::assertSame(30, self::getHttpClient($client)->getConfig('connect_timeout'));
    }

    public function testCustomRequestTimeoutIsUsed(): void
    {
        $client = new AppsApiClient('https://data-apps.keboola.com', 'my-token', requestTimeout: 300);
        self::assertSame(300, self::getHttpClient($client)->getConfig('timeout'));
    }

    public function testDefaultBackoffMaxTriesIsFive(): void
    {
        // The RetryDecider logs "retrying (N of MAX)" on failure. Trigger one 500 to
        // capture the log message and verify MAX == 5 (kills IncrementInteger 5→6 and
        // DecrementInteger 5→4 mutations on the default parameter value).
        $handler = new TestHandler();
        $logger = new Logger('test', [$handler]);

        $mock = new MockHandler([
            new Response(500),
            new Response(200, ['Content-Type' => 'application/json'], Json::encodeArray([])),
        ]);
        [$requestHandler] = self::createCapturingHandler($mock);

        $client = new AppsApiClient(
            'https://data-apps.keboola.com',
            'my-token',
            logger: $logger,
            requestHandler: $requestHandler,
        );
        $client->listApps();

        // RetryDecider logs "retrying (0 of 5)" — verify the configured max is exactly 5
        $records = $handler->getRecords();
        self::assertNotEmpty($records);
        $messages = implode(' ', array_map(
            static fn(object $r): string => (string) $r->message,
            $records,
        ));
        self::assertStringContainsString('of 5', $messages);
    }

    public function testPassedLoggerReceivesLogEntries(): void
    {
        $handler = new TestHandler();
        $logger = new Logger('test', [$handler]);

        $mock = new MockHandler([
            new Response(200, ['Content-Type' => 'application/json'], Json::encodeArray([])),
        ]);
        [$requestHandler] = self::createCapturingHandler($mock);

        $client = new AppsApiClient(
            'https://data-apps.keboola.com',
            'my-token',
            logger: $logger,
            backoffMaxTries: 0,
            requestHandler: $requestHandler,
        );
        $client->listApps();

        self::assertTrue($handler->hasRecords('INFO'), 'Passed logger should receive log entries');
    }
}
