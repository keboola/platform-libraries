<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Tests\Authentication\Authenticator;

use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Middleware;
use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Psr7\Response;
use Keboola\AzureApiClient\ApiClientConfiguration;
use Keboola\AzureApiClient\Authentication\Authenticator\ManagedCredentialsAuth;
use Keboola\AzureApiClient\Exception\ClientException;
use Monolog\Handler\TestHandler;
use Monolog\Logger;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;

class ManagedCredentialsAuthTest extends TestCase
{
    private readonly LoggerInterface $logger;
    private readonly TestHandler $logsHandler;

    protected function setUp(): void
    {
        parent::setUp();

        $this->logsHandler = new TestHandler();
        $this->logger = new Logger('tests', [$this->logsHandler]);
    }

    public function testGetAuthenticationToken(): void
    {
        $requestHandler = self::createRequestHandler($requestsHistory, [
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                '{
                    "token_type": "Bearer",
                    "expires_in": 3599,
                    "resource": "https://vault.azure.net",
                    "access_token": "ey....ey"
                }'
            ),
        ]);

        $auth = new ManagedCredentialsAuth(new ApiClientConfiguration(
            requestHandler: $requestHandler(...),
            logger: $this->logger,
        ));

        $token = $auth->getAuthenticationToken('resource-id');
        self::assertSame('ey....ey', $token->value);
        self::assertEqualsWithDelta(time() + 3599, $token->expiresAt?->getTimestamp(), 1);
        self::assertCount(1, $requestsHistory);

        $request = $requestsHistory[0]['request'];
        self::assertSame(
            // phpcs:ignore Generic.Files.LineLength
            'http://169.254.169.254/metadata/identity/oauth2/token?api-version=2019-11-01&format=text&resource=resource-id',
            $request->getUri()->__toString()
        );
        self::assertSame('GET', $request->getMethod());
        self::assertSame('true', $request->getHeader('Metadata')[0]);
    }

    public function testGetAuthenticationTokenWithInvalidResponse(): void
    {
        $requestHandler = self::createRequestHandler($requestsHistory, [
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                '{
                    "foo": "bar"
                }'
            ),
        ]);

        $auth = new ManagedCredentialsAuth(new ApiClientConfiguration(
            requestHandler: $requestHandler(...),
            logger: $this->logger,
        ));

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage(
            'Failed to map response data: Missing or invalid "access_token" in response: {"foo":"bar"}'
        );
        $auth->getAuthenticationToken('resource-id');
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
}
