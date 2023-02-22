<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Tests\ApiClientFactory;

use DateTimeImmutable;
use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Middleware;
use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Psr7\Response;
use Keboola\AzureApiClient\ApiClientFactory\AuthenticatedAzureApiClientFactory;
use Keboola\AzureApiClient\ApiClientFactory\AuthorizationHeaderResolverInterface;
use Keboola\AzureApiClient\ApiClientFactory\BearerAuthorizationHeaderResolver;
use Keboola\AzureApiClient\Authentication\AuthenticatorFactory;
use Keboola\AzureApiClient\Authentication\AuthenticatorInterface;
use Keboola\AzureApiClient\Authentication\TokenWithExpiration;
use Keboola\AzureApiClient\Json;
use PHPUnit\Framework\TestCase;

class AuthenticatedAzureApiClientFactoryTest extends TestCase
{
    public function testCreatedClientAddsAuthorizationHeader(): void
    {
        $requestHandler = $this->createRequestHandler($requestsHistory, [
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                Json::encodeArray(['foo' => 'bar']),
            ),
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                Json::encodeArray(['foo' => 'bar']),
            ),
        ]);

        $factory = new AuthenticatedAzureApiClientFactory(
            $this->createFakeAuthenticator('auth-token'),
            [
                'requestHandler' => $requestHandler,
            ],
        );
        $client = $factory->createClient('http://example.com', 'foo');

        $client->sendRequest(new Request('GET', '/foo'));
        self::assertCount(1, $requestsHistory);

        $request = $requestsHistory[0]['request'];
        self::assertSame('Bearer auth-token', $request->getHeaderLine('Authorization'));
    }

    /**
     * @param non-empty-string $authToken
     */
    private function createFakeAuthenticator(string $authToken): AuthenticatorInterface
    {
        $authenticator = $this->createMock(AuthenticatorInterface::class);
        $authenticator->expects(self::once())
            ->method('getAuthenticationToken')
            ->willReturn(new TokenWithExpiration(
                $authToken,
                new DateTimeImmutable('+1 hour'),
            ))
        ;
        $authenticator->expects(self::once())->method('getHeaderResolver')
            ->willReturn(
                new BearerAuthorizationHeaderResolver($authenticator, '')
            )
        ;

        return $authenticator;
    }

    /**
     * @param list<array{request: Request, response: Response}> $requestsHistory
     * @param list<Response>                                    $responses
     */
    private function createRequestHandler(?array &$requestsHistory, array $responses): HandlerStack
    {
        $requestsHistory = [];

        $stack = HandlerStack::create(new MockHandler($responses));
        $stack->push(Middleware::history($requestsHistory));

        return $stack;
    }
}
