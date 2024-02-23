<?php

declare(strict_types=1);

namespace Keboola\SandboxesServiceApiClient\Tests\Sandboxes;

use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Middleware;
use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Psr7\Response;
use Keboola\SandboxesServiceApiClient\ApiClientConfiguration;
use Keboola\SandboxesServiceApiClient\Json;
use Keboola\SandboxesServiceApiClient\Sandboxes\SandboxesApiClient;
use PHPUnit\Framework\TestCase;

class SandboxesApiClientTest extends TestCase
{
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
        ];

        $requestHandler = self::createRequestHandler($requestsHistory, [
            new Response(
                201,
                ['Content-Type' => 'application/json'],
                Json::encodeArray($responseBody),
            ),
        ]);

        $client = new SandboxesApiClient(
            new ApiClientConfiguration(
                baseUrl: 'https://data-science.keboola.com',
                storageToken: 'my-token',
                userAgent: 'Keboola Sandboxes Service API PHP Client',
                requestHandler: $requestHandler(...),
            ),
        );
        $result = $client->createSandbox([
            'componentId' => 'component-id',
            'configurationId' => '123',
            'configurationVersion' => '4',
            'type' => 'sandbox-type',
        ]);

        self::assertEquals($responseBody, $result);

        self::assertCount(1, $requestsHistory);
        self::assertRequestEquals(
            'POST',
            'https://data-science.keboola.com/sandboxes',
            [
                'X-StorageApi-Token' => 'my-token',
            ],
            Json::encodeArray([
                'componentId' => 'component-id',
                'configurationId' => '123',
                'configurationVersion' => '4',
                'type' => 'sandbox-type',
            ]),
            $requestsHistory[0]['request'],
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
            'active' => false,
        ];

        $requestHandler = self::createRequestHandler($requestsHistory, [
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                Json::encodeArray($responseBody),
            ),
        ]);

        $client = new SandboxesApiClient(
            new ApiClientConfiguration(
                baseUrl: 'https://data-science.keboola.com',
                storageToken: 'my-token',
                userAgent: 'Keboola Sandboxes Service API PHP Client',
                requestHandler: $requestHandler(...),
            ),
        );
        $result = $client->updateSandbox('sandbox-id', [
            'configurationVersion' => '5',
            'active' => false,
        ]);

        self::assertEquals($responseBody, $result);

        self::assertCount(1, $requestsHistory);
        self::assertRequestEquals(
            'PATCH',
            'https://data-science.keboola.com/sandboxes/sandbox-id',
            [
                'X-StorageApi-Token' => 'my-token',
            ],
            Json::encodeArray([
                'configurationVersion' => '5',
                'active' => false,
            ]),
            $requestsHistory[0]['request'],
        );
    }

    public function testDeleteSandbox(): void
    {
        $requestHandler = self::createRequestHandler($requestsHistory, [
            new Response(204),
        ]);

        $client = new SandboxesApiClient(
            new ApiClientConfiguration(
                baseUrl: 'https://data-science.keboola.com',
                storageToken: 'my-token',
                userAgent: 'Keboola Sandboxes Service API PHP Client',
                requestHandler: $requestHandler(...),
            ),
        );
        $client->deleteSandbox('sandbox-id');

        self::assertCount(1, $requestsHistory);
        self::assertRequestEquals(
            'DELETE',
            'https://data-science.keboola.com/sandboxes/sandbox-id',
            [
                'X-StorageApi-Token' => 'my-token',
            ],
            null,
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
}
