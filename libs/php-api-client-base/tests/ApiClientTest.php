<?php

declare(strict_types=1);

namespace Keboola\ApiClientBase\Tests;

use GuzzleHttp\Exception\ConnectException;
use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Psr7\Response;
use Keboola\ApiClientBase\ApiClient;
use Keboola\ApiClientBase\ApiClientOptions;
use Keboola\ApiClientBase\Auth\ManageApiTokenAuthenticator;
use Keboola\ApiClientBase\Auth\NoAuthAuthenticator;
use Keboola\ApiClientBase\Auth\RequestAuthenticatorInterface;
use Keboola\ApiClientBase\ErrorMessageResolverInterface;
use Keboola\ApiClientBase\Exception\ClientException;
use Keboola\ApiClientBase\Tests\Fixtures\DummyClientException;
use Keboola\ApiClientBase\Tests\Fixtures\DummyModel;
use PHPUnit\Framework\TestCase;
use Psr\Http\Message\RequestInterface;
use RuntimeException;

class ApiClientTest extends TestCase
{
    public function testSendsWithoutAuthHeaderWhenNoAuthAuthenticator(): void
    {
        $mock = new MockHandler([new Response(200, [], '{}')]);
        $client = new ApiClient('https://example.test', new NoAuthAuthenticator(), new ApiClientOptions(
            requestHandler: HandlerStack::create($mock),
        ));
        $client->sendRequest(new Request('GET', 'foo'));

        $last = $mock->getLastRequest();
        self::assertNotNull($last);
        self::assertSame([], $last->getHeader('X-KBC-ManageApiToken'));
        self::assertStringContainsString('Keboola PHP API Client', $last->getHeaderLine('User-Agent'));
    }

    public function testAddsAuthHeaderPerRequest(): void
    {
        $mock = new MockHandler([new Response(200, [], '{}')]);
        $client = new ApiClient(
            'https://example.test',
            new ManageApiTokenAuthenticator('secret-token'),
            new ApiClientOptions(requestHandler: HandlerStack::create($mock)),
        );
        $client->sendRequest(new Request('GET', 'foo'));

        $last = $mock->getLastRequest();
        self::assertNotNull($last);
        self::assertSame('secret-token', $last->getHeaderLine('X-KBC-ManageApiToken'));
    }

    public function testSendRequestReturnsResponse(): void
    {
        $mock = new MockHandler([new Response(201, [], '{"hello":"world"}')]);
        $client = new ApiClient('https://example.test', new NoAuthAuthenticator(), new ApiClientOptions(
            requestHandler: HandlerStack::create($mock),
        ));

        $response = $client->sendRequest(new Request('GET', 'foo'));

        self::assertSame(201, $response->getStatusCode());
        self::assertSame('{"hello":"world"}', (string) $response->getBody());
    }

    public function testMapsResponseToModel(): void
    {
        $mock = new MockHandler([new Response(200, [], '{"name":"foo"}')]);
        $client = new ApiClient('https://example.test', new NoAuthAuthenticator(), new ApiClientOptions(
            requestHandler: HandlerStack::create($mock),
        ));
        $model = $client->sendRequestAndMapResponse(new Request('GET', 'foo'), DummyModel::class);

        self::assertInstanceOf(DummyModel::class, $model);
        self::assertSame('foo', $model->name);
    }

    public function testMapsResponseToList(): void
    {
        $mock = new MockHandler([new Response(200, [], '[{"name":"a"},{"name":"b"}]')]);
        $client = new ApiClient('https://example.test', new NoAuthAuthenticator(), new ApiClientOptions(
            requestHandler: HandlerStack::create($mock),
        ));
        $models = $client->sendRequestAndMapResponse(new Request('GET', 'foo'), DummyModel::class, [], true);

        self::assertCount(2, $models);
        self::assertSame('a', $models[0]->name);
        self::assertSame('b', $models[1]->name);
    }

    public function testRetriesOn5xxThenSucceeds(): void
    {
        $mock = new MockHandler([new Response(500), new Response(200, [], '{}')]);
        $client = new ApiClient('https://example.test', new NoAuthAuthenticator(), new ApiClientOptions(
            requestHandler: HandlerStack::create($mock),
        ));
        $client->sendRequest(new Request('GET', 'foo'));
        self::assertSame(0, $mock->count());
    }

    public function testThrowsClientExceptionWithDefaultMessageExtraction(): void
    {
        $mock = new MockHandler([new Response(400, [], '{"error":"bad input"}')]);
        $client = new ApiClient('https://example.test', new NoAuthAuthenticator(), new ApiClientOptions(
            requestHandler: HandlerStack::create($mock),
        ));
        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('bad input');
        $this->expectExceptionCode(400);
        $client->sendRequest(new Request('GET', 'foo'));
    }

    public function testUsesCustomErrorMessageResolver(): void
    {
        $mock = new MockHandler([new Response(409, [], '{"code":"CONFLICT","error":"already exists"}')]);
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
            new ApiClientOptions(requestHandler: HandlerStack::create($mock)),
            errorMessageResolver: $resolver,
        );
        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('CONFLICT: already exists');
        $client->sendRequest(new Request('GET', 'foo'));
    }

    public function testReExecutesAuthenticatorOnEachRetryAttempt(): void
    {
        $authenticator = new class implements RequestAuthenticatorInterface {
            public int $calls = 0;

            public function __invoke(RequestInterface $request): RequestInterface
            {
                $this->calls++;
                return $request->withHeader('X-Attempt', 'call-' . $this->calls);
            }
        };

        $mock = new MockHandler([new Response(500), new Response(200, [], '{}')]);
        $client = new ApiClient(
            'https://example.test',
            $authenticator,
            new ApiClientOptions(backoffMaxTries: 2, requestHandler: HandlerStack::create($mock)),
        );
        $client->sendRequest(new Request('GET', 'foo'));

        self::assertSame(2, $authenticator->calls);
        $last = $mock->getLastRequest();
        self::assertNotNull($last);
        self::assertSame('call-2', $last->getHeaderLine('X-Attempt'));
    }

    public function testThrowsClientExceptionAfterRetriesExhausted(): void
    {
        $mock = new MockHandler([new Response(500), new Response(500)]);
        $client = new ApiClient('https://example.test', new NoAuthAuthenticator(), new ApiClientOptions(
            backoffMaxTries: 1,
            requestHandler: HandlerStack::create($mock),
        ));
        $this->expectException(ClientException::class);
        $this->expectExceptionCode(500);
        $client->sendRequest(new Request('GET', 'foo'));
    }

    public function testRetriesConfiguredStatusCodeThroughClient(): void
    {
        $mock = new MockHandler([new Response(429), new Response(200, [], '{}')]);
        $client = new ApiClient(
            'https://example.test',
            new NoAuthAuthenticator(),
            new ApiClientOptions(backoffMaxTries: 2, requestHandler: HandlerStack::create($mock)),
            retryableStatusCodes: [429],
        );
        $client->sendRequest(new Request('GET', 'foo'));
        self::assertSame(0, $mock->count());
    }

    public function testAuthenticatorFailureIsRetriedAndSurfacesAsClientException(): void
    {
        // An authenticator that throws (e.g. a projected SA token momentarily unreadable)
        // must surface as ClientException and flow through retry — not escape as a raw
        // RuntimeException that bypasses both error handling and retry.
        $authenticator = new class implements RequestAuthenticatorInterface {
            public int $calls = 0;

            public function __invoke(RequestInterface $request): RequestInterface
            {
                $this->calls++;
                throw new RuntimeException('SA token file not readable');
            }
        };

        $mock = new MockHandler([new Response(200), new Response(200)]);
        $client = new ApiClient(
            'https://example.test',
            $authenticator,
            new ApiClientOptions(backoffMaxTries: 1, requestHandler: HandlerStack::create($mock)),
        );

        try {
            $client->sendRequest(new Request('GET', 'foo'));
            self::fail('Expected ClientException to be thrown');
        } catch (ClientException $e) {
            self::assertStringContainsString('SA token file not readable', $e->getMessage());
        }

        // initial attempt + one retry — the auth failure went through RetryDecider
        self::assertSame(2, $authenticator->calls);
    }

    public function testThrowsConfiguredExceptionClass(): void
    {
        $mock = new MockHandler([new Response(400, [], '{"error":"bad"}')]);
        $client = new ApiClient(
            'https://example.test',
            new NoAuthAuthenticator(),
            new ApiClientOptions(requestHandler: HandlerStack::create($mock)),
            exceptionClass: DummyClientException::class,
        );

        try {
            $client->sendRequest(new Request('GET', 'foo'));
            self::fail('Expected exception');
        } catch (ClientException $e) {
            self::assertInstanceOf(DummyClientException::class, $e);
        }
    }

    public function testPopulatesStatusCodeAndResponseBodyOnHttpError(): void
    {
        $mock = new MockHandler([new Response(409, [], '{"error":"conflict"}')]);
        $client = new ApiClient(
            'https://example.test',
            new NoAuthAuthenticator(),
            new ApiClientOptions(requestHandler: HandlerStack::create($mock)),
        );

        try {
            $client->sendRequest(new Request('GET', 'foo'));
            self::fail('Expected exception');
        } catch (ClientException $e) {
            self::assertSame(409, $e->getStatusCode());
            self::assertSame('{"error":"conflict"}', $e->getResponseBody());
        }
    }

    public function testStatusCodeNullOnTransportError(): void
    {
        $mock = new MockHandler([
            new ConnectException('Connection refused', new Request('GET', 'foo')),
        ]);
        $client = new ApiClient(
            'https://example.test',
            new NoAuthAuthenticator(),
            new ApiClientOptions(backoffMaxTries: 0, requestHandler: HandlerStack::create($mock)),
        );

        try {
            $client->sendRequest(new Request('GET', 'foo'));
            self::fail('Expected exception');
        } catch (ClientException $e) {
            self::assertNull($e->getStatusCode());
            self::assertNull($e->getResponseBody());
        }
    }

    public function testDefaultExceptionOriginatesInApiClient(): void
    {
        // Clean-trace guard: the exception is constructed inline at the throw site, so its
        // origin stays in ApiClient rather than in a factory/builder frame.
        $mock = new MockHandler([new Response(400, [], 'bad')]);
        $client = new ApiClient(
            'https://example.test',
            new NoAuthAuthenticator(),
            new ApiClientOptions(requestHandler: HandlerStack::create($mock)),
        );

        try {
            $client->sendRequest(new Request('GET', 'foo'));
            self::fail('Expected exception');
        } catch (ClientException $e) {
            self::assertStringContainsString('ApiClient.php', $e->getFile());
        }
    }

    public function testPopulatesContextOnInvalidJsonResponse(): void
    {
        $mock = new MockHandler([new Response(200, [], 'not json')]);
        $client = new ApiClient(
            'https://example.test',
            new NoAuthAuthenticator(),
            new ApiClientOptions(requestHandler: HandlerStack::create($mock)),
        );

        try {
            $client->sendRequestAndMapResponse(new Request('GET', 'foo'), DummyModel::class);
            self::fail('Expected exception');
        } catch (ClientException $e) {
            self::assertStringContainsString('Response is not valid JSON', $e->getMessage());
            self::assertSame(200, $e->getStatusCode());
            self::assertSame('not json', $e->getResponseBody());
        }
    }

    public function testPopulatesContextOnMappingFailure(): void
    {
        // DummyModel::fromResponseData asserts is_string($data['name']); passing an integer
        // triggers AssertionError (a Throwable), exercising the mapping-failure branch.
        $body = '{"name":123}';
        $mock = new MockHandler([new Response(200, [], $body)]);
        $client = new ApiClient(
            'https://example.test',
            new NoAuthAuthenticator(),
            new ApiClientOptions(requestHandler: HandlerStack::create($mock)),
        );

        try {
            $client->sendRequestAndMapResponse(new Request('GET', 'foo'), DummyModel::class);
            self::fail('Expected exception');
        } catch (ClientException $e) {
            self::assertStringContainsString('Failed to map response data', $e->getMessage());
            self::assertSame(200, $e->getStatusCode());
            self::assertSame($body, $e->getResponseBody());
        }
    }
}
