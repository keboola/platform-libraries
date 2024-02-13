<?php

declare(strict_types=1);

namespace Keboola\SandboxesServiceApiClient\Tests\Sandboxes;

use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Middleware;
use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Psr7\Response;
use Keboola\SandboxesServiceApiClient\ApiClientConfiguration;
use Keboola\SandboxesServiceApiClient\Authentication\StorageTokenAuthenticator;
use Keboola\SandboxesServiceApiClient\Json;
use Keboola\SandboxesServiceApiClient\Sandboxes\Model\CreateSandboxPayload;
use Keboola\SandboxesServiceApiClient\Sandboxes\Model\CreateSandboxResult;
use Keboola\SandboxesServiceApiClient\Sandboxes\SandboxesClient;
use PHPUnit\Framework\TestCase;

class SandboxesClientTest extends TestCase
{
    public function testCreateSandboxesClient(): void
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
                200,
                ['Content-Type' => 'application/json'],
                Json::encodeArray($responseBody),
            ),
        ]);

        $client = new SandboxesClient(
            configuration: new ApiClientConfiguration(
                authenticator: new StorageTokenAuthenticator('my-token'),
                requestHandler: $requestHandler(...),
            ),
        );
        $result = $client->createSandbox(new CreateSandboxPayload(
            'component-id',
            '123',
            '4',
            'sandbox-type',
        ));

        self::assertEquals(
            CreateSandboxResult::fromResponseData($responseBody),
            $result,
        );

        self::assertCount(1, $requestsHistory);
        self::assertRequestEquals(
            'POST',
            '/sandboxes',
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

        $client = new SandboxesClient();
        $this->assertInstanceOf(SandboxesClient::class, $client);
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
