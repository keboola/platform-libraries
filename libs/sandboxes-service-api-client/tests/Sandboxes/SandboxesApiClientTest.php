<?php

declare(strict_types=1);

namespace Keboola\SandboxesServiceApiClient\Tests\Sandboxes;

use GuzzleHttp\Client as GuzzleClient;
use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\Psr7\Response;
use InvalidArgumentException;
use Keboola\ApiClientBase\ApiClient;
use Keboola\ApiClientBase\Exception\ClientException;
use Keboola\ApiClientBase\Json;
use Keboola\SandboxesServiceApiClient\Sandboxes\Legacy\Project;
use Keboola\SandboxesServiceApiClient\Sandboxes\Legacy\Sandbox;
use Keboola\SandboxesServiceApiClient\Sandboxes\SandboxesApiClient;
use Keboola\SandboxesServiceApiClient\Tests\ReflectionPropertyAccessTestCase;
use Monolog\Handler\TestHandler;
use Monolog\Logger;
use PHPUnit\Framework\TestCase;
use Psr\Http\Message\RequestInterface;
use stdClass;

class SandboxesApiClientTest extends TestCase
{
    use ReflectionPropertyAccessTestCase;

    public function testGetSandbox(): void
    {
        $responseBody = [
            'id' => 'sandbox-id',
            'projectId' => 'project-id',
            'tokenId' => 'token-id',
            'componentId' => 'component-id-2',
            'configurationId' => '124',
            'configurationVersion' => '5',
            'type' => 'sandbox-type',
            'branchId' => null,
            'active' => false,
            'shared' => false,
            'persistentStorage' => [
                'pvcName' => null,
                'k8sManifest' => null,
            ],
            'createdTimestamp' => '2024-02-01T08:00:00+01:00',
        ];

        $mock = new MockHandler([
            new Response(200, ['Content-Type' => 'application/json'], Json::encodeArray($responseBody)),
        ]);
        [$requestHandler, $store] = self::createCapturingHandler($mock);

        $client = new SandboxesApiClient(
            'https://data-science.keboola.com',
            'my-token',
            backoffMaxTries: 0,
            requestHandler: $requestHandler,
        );
        $result = $client->getSandbox('sandbox-id');

        self::assertEquals(Sandbox::fromResponseData($responseBody), $result);

        self::assertCount(1, $store->requests);
        self::assertRequestEquals(
            'GET',
            'https://data-science.keboola.com/sandboxes/sandbox-id',
            [
                'X-StorageApi-Token' => 'my-token',
            ],
            '',
            $store->requests[0],
        );
    }

    public function testCreateSandbox(): void
    {
        $responseBody = [
            'id' => 'sandbox-id',
            'projectId' => 'project-id',
            'tokenId' => 'token-id',
            'componentId' => 'component-id',
            'configurationId' => '123',
            'configurationVersion' => '4',
            'type' => 'sandbox-type',
            'branchId' => null,
            'active' => false,
            'shared' => false,
            'persistentStorage' => [
                'pvcName' => null,
                'k8sManifest' => null,
            ],
            'createdTimestamp' => '2024-02-01T08:00:00+01:00',
        ];

        $mock = new MockHandler([
            new Response(201, ['Content-Type' => 'application/json'], Json::encodeArray($responseBody)),
        ]);
        [$requestHandler, $store] = self::createCapturingHandler($mock);

        $client = new SandboxesApiClient(
            'https://data-science.keboola.com',
            'my-token',
            backoffMaxTries: 0,
            requestHandler: $requestHandler,
        );
        $result = $client->createSandbox([
            'componentId' => 'component-id',
            'configurationId' => '123',
            'configurationVersion' => '4',
            'type' => 'sandbox-type',
        ]);

        self::assertEquals(Sandbox::fromResponseData($responseBody), $result);

        self::assertCount(1, $store->requests);
        self::assertRequestEquals(
            'POST',
            'https://data-science.keboola.com/sandboxes',
            [
                'X-StorageApi-Token' => 'my-token',
                'Content-Type' => 'application/json',
            ],
            Json::encodeArray([
                'componentId' => 'component-id',
                'configurationId' => '123',
                'configurationVersion' => '4',
                'type' => 'sandbox-type',
            ]),
            $store->requests[0],
        );
    }

    public function testUpdateSandbox(): void
    {
        $responseBody = [
            'id' => 'sandbox-id',
            'projectId' => 'project-id',
            'tokenId' => 'token-id',
            'componentId' => 'component-id-2',
            'configurationId' => '124',
            'configurationVersion' => '5',
            'type' => 'sandbox-type',
            'branchId' => null,
            'active' => false,
            'shared' => false,
            'persistentStorage' => [
                'pvcName' => null,
                'k8sManifest' => null,
            ],
            'createdTimestamp' => '2024-02-01T08:00:00+01:00',
        ];

        $mock = new MockHandler([
            new Response(200, ['Content-Type' => 'application/json'], Json::encodeArray($responseBody)),
        ]);
        [$requestHandler, $store] = self::createCapturingHandler($mock);

        $client = new SandboxesApiClient(
            'https://data-science.keboola.com',
            'my-token',
            backoffMaxTries: 0,
            requestHandler: $requestHandler,
        );
        $result = $client->updateSandbox('sandbox-id', [
            'configurationVersion' => '5',
            'active' => false,
        ]);

        self::assertEquals(Sandbox::fromResponseData($responseBody), $result);

        self::assertCount(1, $store->requests);
        self::assertRequestEquals(
            'PATCH',
            'https://data-science.keboola.com/sandboxes/sandbox-id',
            [
                'X-StorageApi-Token' => 'my-token',
                'Content-Type' => 'application/json',
            ],
            Json::encodeArray([
                'configurationVersion' => '5',
                'active' => false,
            ]),
            $store->requests[0],
        );
    }

    public function testDeleteSandbox(): void
    {
        $mock = new MockHandler([new Response(204)]);
        [$requestHandler, $store] = self::createCapturingHandler($mock);

        $client = new SandboxesApiClient(
            'https://data-science.keboola.com',
            'my-token',
            backoffMaxTries: 0,
            requestHandler: $requestHandler,
        );
        $client->deleteSandbox('sandbox-id');

        self::assertCount(1, $store->requests);
        self::assertRequestEquals(
            'DELETE',
            'https://data-science.keboola.com/sandboxes/sandbox-id',
            [
                'X-StorageApi-Token' => 'my-token',
            ],
            null,
            $store->requests[0],
        );
    }

    public function testGetCurrentProject(): void
    {
        $responseBody = [
            'id' => '123',
            'createdTimestamp' => '2024-02-01T08:00:00+01:00',
        ];

        $mock = new MockHandler([
            new Response(200, ['Content-Type' => 'application/json'], Json::encodeArray($responseBody)),
        ]);
        [$requestHandler, $store] = self::createCapturingHandler($mock);

        $client = new SandboxesApiClient(
            'https://data-science.keboola.com',
            'my-token',
            backoffMaxTries: 0,
            requestHandler: $requestHandler,
        );
        $result = $client->getCurrentProject();

        self::assertEquals(Project::fromResponseData($responseBody), $result);

        self::assertCount(1, $store->requests);
        self::assertRequestEquals(
            'GET',
            'https://data-science.keboola.com/sandboxes/project',
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
        $responseBody = [
            'id' => '123',
            'createdTimestamp' => '2024-02-01T08:00:00+01:00',
        ];

        $mock = new MockHandler([
            new Response(200, ['Content-Type' => 'application/json'], Json::encodeArray($responseBody)),
        ]);
        [$requestHandler, $store] = self::createCapturingHandler($mock);

        $client = new SandboxesApiClient(
            'https://data-science.keboola.com',
            'my-token',
            backoffMaxTries: 0,
            userAgent: 'Custom Agent/1.0',
            requestHandler: $requestHandler,
        );
        $client->getCurrentProject();

        self::assertCount(1, $store->requests);
        self::assertSame(
            'Custom Agent/1.0',
            $store->requests[0]->getHeaderLine('User-Agent'),
        );
    }

    public function testDefaultOptionsWithRetry(): void
    {
        $responseBody = [
            'id' => 'sandbox-id',
            'projectId' => 'project-id',
            'tokenId' => 'token-id',
            'componentId' => 'component-id-2',
            'configurationId' => '124',
            'configurationVersion' => '5',
            'type' => 'sandbox-type',
            'branchId' => null,
            'active' => false,
            'shared' => false,
            'persistentStorage' => [
                'pvcName' => null,
                'k8sManifest' => null,
            ],
            'createdTimestamp' => '2024-02-01T08:00:00+01:00',
        ];

        $mock = new MockHandler([
            new Response(500),
            new Response(200, ['Content-Type' => 'application/json'], Json::encodeArray($responseBody)),
        ]);
        [$requestHandler, $store] = self::createCapturingHandler($mock);

        $client = new SandboxesApiClient(
            'https://data-science.keboola.com',
            'my-token',
            requestHandler: $requestHandler,
        );
        // Should succeed after retry
        $result = $client->getSandbox('sandbox-id');
        self::assertInstanceOf(Sandbox::class, $result);
        self::assertCount(2, $store->requests);
    }

    public function testConstructWithNullOptionsDoesNotThrow(): void
    {
        // With null options, the facade should use defaults without throwing TypeError
        $client = new SandboxesApiClient(
            'https://data-science.keboola.com',
            'my-token',
        );
        self::assertInstanceOf(SandboxesApiClient::class, $client);
    }

    public function testEmptyTokenThrows(): void
    {
        $this->expectException(InvalidArgumentException::class);
        // @phpstan-ignore argument.type
        new SandboxesApiClient('https://data-science.keboola.com', '');
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

        $client = new SandboxesApiClient(
            'https://data-science.keboola.com',
            'my-token',
            backoffMaxTries: 0,
            requestHandler: $requestHandler,
        );

        try {
            $client->getSandbox('sandbox-id');
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

        $client = new SandboxesApiClient(
            'https://data-science.keboola.com',
            'my-token',
            backoffMaxTries: 0,
            requestHandler: $requestHandler,
        );

        try {
            $client->getSandbox('sandbox-id');
            self::fail('Expected ClientException');
        } catch (ClientException $e) {
            // trim() should remove trailing space
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

        $client = new SandboxesApiClient(
            'https://data-science.keboola.com',
            'my-token',
            backoffMaxTries: 0,
            requestHandler: $requestHandler,
        );

        try {
            $client->getSandbox('sandbox-id');
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

        $client = new SandboxesApiClient(
            'https://data-science.keboola.com',
            'my-token',
            backoffMaxTries: 0,
            requestHandler: $requestHandler,
        );

        try {
            $client->getSandbox('sandbox-id');
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

        $client = new SandboxesApiClient(
            'https://data-science.keboola.com',
            'my-token',
            backoffMaxTries: 0,
            requestHandler: $requestHandler,
        );

        try {
            $client->getSandbox('sandbox-id');
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

        $client = new SandboxesApiClient(
            'https://data-science.keboola.com',
            'my-token',
            backoffMaxTries: 0,
            requestHandler: $requestHandler,
        );

        try {
            $client->getSandbox('sandbox-id');
            self::fail('Expected ClientException');
        } catch (ClientException $e) {
            // Empty message field → should NOT produce "BadRequest: " format
            self::assertStringNotContainsString('BadRequest: ', $e->getMessage());
        }
    }

    private static function getHttpClient(SandboxesApiClient $client): GuzzleClient
    {
        $apiClient = self::getPrivatePropertyValue($client, 'apiClient');
        self::assertInstanceOf(ApiClient::class, $apiClient);
        $httpClient = self::getPrivatePropertyValue($apiClient, 'httpClient');
        self::assertInstanceOf(GuzzleClient::class, $httpClient);
        return $httpClient;
    }

    public function testDefaultConnectTimeoutIs10(): void
    {
        $client = new SandboxesApiClient('https://data-science.keboola.com', 'my-token');
        self::assertSame(10, self::getHttpClient($client)->getConfig('connect_timeout'));
    }

    public function testDefaultRequestTimeoutIs120(): void
    {
        $client = new SandboxesApiClient('https://data-science.keboola.com', 'my-token');
        self::assertSame(120, self::getHttpClient($client)->getConfig('timeout'));
    }

    public function testCustomConnectTimeoutIsUsed(): void
    {
        $client = new SandboxesApiClient('https://data-science.keboola.com', 'my-token', connectTimeout: 30);
        self::assertSame(30, self::getHttpClient($client)->getConfig('connect_timeout'));
    }

    public function testCustomRequestTimeoutIsUsed(): void
    {
        $client = new SandboxesApiClient('https://data-science.keboola.com', 'my-token', requestTimeout: 300);
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
            new Response(200, ['Content-Type' => 'application/json'], Json::encodeArray([
                'id' => '123',
                'createdTimestamp' => '2024-02-01T08:00:00+01:00',
            ])),
        ]);
        [$requestHandler] = self::createCapturingHandler($mock);

        $client = new SandboxesApiClient(
            'https://data-science.keboola.com',
            'my-token',
            logger: $logger,
            requestHandler: $requestHandler,
        );
        $client->getCurrentProject();

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
            new Response(200, ['Content-Type' => 'application/json'], Json::encodeArray([
                'id' => '123',
                'createdTimestamp' => '2024-02-01T08:00:00+01:00',
            ])),
        ]);
        [$requestHandler] = self::createCapturingHandler($mock);

        $client = new SandboxesApiClient(
            'https://data-science.keboola.com',
            'my-token',
            logger: $logger,
            backoffMaxTries: 0,
            requestHandler: $requestHandler,
        );
        $client->getCurrentProject();

        self::assertTrue($handler->hasRecords('INFO'), 'Passed logger should receive log entries');
    }
}
