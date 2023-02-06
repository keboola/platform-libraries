<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Tests\ApiClientFactory;

use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Middleware;
use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Psr7\Response;
use Keboola\AzureApiClient\ApiClientFactory\AuthenticatedAzureApiClientFactory;
use Keboola\AzureApiClient\Authentication\AuthenticatorFactory;
use Keboola\AzureApiClient\Authentication\AuthenticatorInterface;
use Keboola\AzureApiClient\GuzzleClientFactory;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

class AuthenticatedAzureApiClientFactoryTest extends TestCase
{
    private readonly LoggerInterface $logger;

    protected function setUp(): void
    {
        parent::setUp();

        $this->logger = new NullLogger();
    }

    public function testCreatedClientAddsAuthorizationHeader(): void
    {
        $guzzleClientFactory = $this->createGuzzleClientFactory($requestsHistory, [
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                (string) json_encode(['foo' => 'bar']),
            ),
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                (string) json_encode(['foo' => 'bar']),
            ),
        ]);

        $factory = new AuthenticatedAzureApiClientFactory(
            $guzzleClientFactory,
            $this->createFakeAuthenticatorFactory('auth-token'),
        );
        $client = $factory->createClient('http://example.com', 'foo');

        $client->sendRequest(new Request('GET', '/foo'));
        self::assertCount(1, $requestsHistory);

        $request = $requestsHistory[0]['request'];
        self::assertSame('Bearer auth-token', $request->getHeaderLine('Authorization'));

        // second request does not request new access token
        $client->sendRequest(new Request('GET', '/foo'));
        self::assertCount(2, $requestsHistory);

        $request = $requestsHistory[1]['request'];
        self::assertSame('Bearer auth-token', $request->getHeaderLine('Authorization'));
    }

    private function createFakeAuthenticatorFactory(string $authToken): AuthenticatorFactory
    {
        $authenticator = $this->createMock(AuthenticatorInterface::class);
        $authenticator->expects(self::once())
            ->method('getAuthenticationToken')
            ->willReturn($authToken)
        ;

        $authenticatorFactory = $this->createMock(AuthenticatorFactory::class);
        $authenticatorFactory->expects(self::once())
            ->method('createAuthenticator')
            ->willReturn($authenticator)
        ;

        return $authenticatorFactory;
    }

    /**
     * @param list<array{request: Request, response: Response}> $requestsHistory
     * @param list<Response>                                    $responses
     */
    private function createGuzzleClientFactory(?array &$requestsHistory, array $responses): GuzzleClientFactory
    {
        $requestsHistory = [];

        $stack = HandlerStack::create(new MockHandler($responses));
        $stack->push(Middleware::history($requestsHistory));

        return new GuzzleClientFactory($this->logger, $stack);
    }
}
