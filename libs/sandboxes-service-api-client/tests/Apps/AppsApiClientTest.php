<?php

declare(strict_types=1);

namespace Keboola\SandboxesServiceApiClient\Tests\Apps;

use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Middleware;
use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Psr7\Response;
use Keboola\SandboxesServiceApiClient\ApiClientConfiguration;
use Keboola\SandboxesServiceApiClient\Apps\App;
use Keboola\SandboxesServiceApiClient\Apps\AppsApiClient;
use Keboola\SandboxesServiceApiClient\Json;
use PHPUnit\Framework\TestCase;

class AppsApiClientTest extends TestCase
{
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

        $requestHandler = self::createRequestHandler($requestsHistory, [
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                Json::encodeArray($responseBody),
            ),
        ]);

        $client = new AppsApiClient(
            new ApiClientConfiguration(
                baseUrl: 'https://data-apps.keboola.com',
                storageToken: 'my-token',
                userAgent: 'Keboola Sandboxes Service API PHP Client',
                requestHandler: $requestHandler(...),
            ),
        );
        $result = $client->listApps();

        $expectedApps = [
            App::fromArray($responseBody[0]),
            App::fromArray($responseBody[1]),
        ];
        self::assertEquals($expectedApps, $result);

        self::assertCount(1, $requestsHistory);
        self::assertRequestEquals(
            'GET',
            'https://data-apps.keboola.com/apps',
            [
                'X-StorageApi-Token' => 'my-token',
            ],
            '',
            $requestsHistory[0]['request'],
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

        $requestHandler = self::createRequestHandler($requestsHistory, [
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                Json::encodeArray($responseBody),
            ),
        ]);

        $client = new AppsApiClient(
            new ApiClientConfiguration(
                baseUrl: 'https://data-apps.keboola.com',
                storageToken: 'my-token',
                userAgent: 'Keboola Sandboxes Service API PHP Client',
                requestHandler: $requestHandler(...),
            ),
        );
        $result = $client->listApps(10, 50);

        $expectedApps = [App::fromArray($responseBody[0])];
        self::assertEquals($expectedApps, $result);

        self::assertCount(1, $requestsHistory);
        self::assertRequestEquals(
            'GET',
            'https://data-apps.keboola.com/apps?offset=10&limit=50',
            [
                'X-StorageApi-Token' => 'my-token',
            ],
            '',
            $requestsHistory[0]['request'],
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

        $requestHandler = self::createRequestHandler($requestsHistory, [
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                Json::encodeArray($responseBody),
            ),
        ]);

        $client = new AppsApiClient(
            new ApiClientConfiguration(
                baseUrl: 'https://data-apps.keboola.com',
                storageToken: 'my-token',
                userAgent: 'Keboola Sandboxes Service API PHP Client',
                requestHandler: $requestHandler(...),
            ),
        );
        $result = $client->getApp('app-id');

        self::assertEquals(App::fromArray($responseBody), $result);

        self::assertCount(1, $requestsHistory);
        self::assertRequestEquals(
            'GET',
            'https://data-apps.keboola.com/apps/app-id',
            [
                'X-StorageApi-Token' => 'my-token',
            ],
            '',
            $requestsHistory[0]['request'],
        );
    }

    public function testPatchApp(): void
    {
        $requestHandler = self::createRequestHandler($requestsHistory, [
            new Response(200),
        ]);

        $client = new AppsApiClient(
            new ApiClientConfiguration(
                baseUrl: 'https://data-apps.keboola.com',
                storageToken: 'my-token',
                userAgent: 'Keboola Sandboxes Service API PHP Client',
                requestHandler: $requestHandler(...),
            ),
        );
        $client->patchApp('app-id', [
            'desiredState' => 'stopped',
            'restartIfRunning' => false,
        ]);

        self::assertCount(1, $requestsHistory);
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
            $requestsHistory[0]['request'],
        );
    }

    /**
     * @param list<array{request: Request, response: Response}> $requestsHistory
     * @param list<Response>                                    $responses
     * @return HandlerStack
     */
    private static function createRequestHandler(?array &$requestsHistory, array $responses): HandlerStack
    {
        $requestsHistory = [];

        $stack = HandlerStack::create(new MockHandler($responses));
        $stack->push(Middleware::history($requestsHistory));

        return $stack;
    }

    private static function assertRequestEquals(
        string $method,
        string $uri,
        array $headers,
        ?string $body,
        Request $request,
    ): void {
        self::assertSame($method, $request->getMethod());
        self::assertSame($uri, $request->getUri()->__toString());

        foreach ($headers as $headerName => $headerValue) {
            self::assertSame($headerValue, $request->getHeaderLine($headerName));
        }

        self::assertSame($body ?? '', $request->getBody()->getContents());
    }

    public function testListAppsWithOnlyOffset(): void
    {
        $responseBody = [];

        $requestHandler = self::createRequestHandler($requestsHistory, [
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                Json::encodeArray($responseBody),
            ),
        ]);

        $client = new AppsApiClient(
            new ApiClientConfiguration(
                baseUrl: 'https://data-apps.keboola.com',
                storageToken: 'my-token',
                userAgent: 'Keboola Sandboxes Service API PHP Client',
                requestHandler: $requestHandler(...),
            ),
        );
        $result = $client->listApps(10);

        self::assertEquals([], $result);

        self::assertCount(1, $requestsHistory);
        self::assertRequestEquals(
            'GET',
            'https://data-apps.keboola.com/apps?offset=10',
            [
                'X-StorageApi-Token' => 'my-token',
            ],
            '',
            $requestsHistory[0]['request'],
        );
    }

    public function testListAppsWithOnlyLimit(): void
    {
        $responseBody = [];

        $requestHandler = self::createRequestHandler($requestsHistory, [
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                Json::encodeArray($responseBody),
            ),
        ]);

        $client = new AppsApiClient(
            new ApiClientConfiguration(
                baseUrl: 'https://data-apps.keboola.com',
                storageToken: 'my-token',
                userAgent: 'Keboola Sandboxes Service API PHP Client',
                requestHandler: $requestHandler(...),
            ),
        );
        $result = $client->listApps(null, 50);

        self::assertEquals([], $result);

        self::assertCount(1, $requestsHistory);
        self::assertRequestEquals(
            'GET',
            'https://data-apps.keboola.com/apps?limit=50',
            [
                'X-StorageApi-Token' => 'my-token',
            ],
            '',
            $requestsHistory[0]['request'],
        );
    }
}
