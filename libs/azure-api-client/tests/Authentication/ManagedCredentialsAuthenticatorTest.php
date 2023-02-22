<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Tests\Authentication;

use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Middleware;
use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Psr7\Response;
use Keboola\AzureApiClient\ApiClientFactory\UnauthenticatedAzureApiClientFactory;
use Keboola\AzureApiClient\Authentication\ManagedCredentialsAuthenticator;
use Keboola\AzureApiClient\Exception\ClientException;
use Monolog\Handler\TestHandler;
use Monolog\Logger;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;

class ManagedCredentialsAuthenticatorTest extends TestCase
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

        $apiClientFactory = new UnauthenticatedAzureApiClientFactory([
            'requestHandler' => $requestHandler,
        ]);

        $auth = new ManagedCredentialsAuthenticator($apiClientFactory, $this->logger);

        $token = $auth->getAuthenticationToken('resource-id');
        self::assertSame('ey....ey', $token->getToken());
        self::assertTrue($token->isValid());
        self::assertCount(1, $requestsHistory);

        $request = $requestsHistory[0]['request'];
        self::assertSame(
            // phpcs:ignore Generic.Files.LineLength
            'http://169.254.169.254/metadata/identity/oauth2/token?api-version=2019-11-01&format=text&resource=resource-id',
            $request->getUri()->__toString()
        );
        self::assertSame('GET', $request->getMethod());
        self::assertSame('true', $request->getHeader('Metadata')[0]);

        self::assertTrue($this->logsHandler->hasInfo('Successfully authenticated using instance metadata.'));
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

        $apiClientFactory = new UnauthenticatedAzureApiClientFactory([
            'requestHandler' => $requestHandler,
        ]);

        $auth = new ManagedCredentialsAuthenticator($apiClientFactory, $this->logger);

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
