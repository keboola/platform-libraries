<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Tests\Authentication;

use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Middleware;
use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Psr7\Response;
use Keboola\AzureApiClient\ApiClientFactory\PlainAzureApiClientFactory;
use Keboola\AzureApiClient\Authentication\AuthenticatorFactory;
use Keboola\AzureApiClient\Authentication\ClientCredentialsEnvironmentAuthenticator;
use Keboola\AzureApiClient\Authentication\ManagedCredentialsAuthenticator;
use Keboola\AzureApiClient\GuzzleClientFactory;
use Monolog\Handler\TestHandler;
use Monolog\Logger;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;

class AuthenticationFactoryTest extends TestCase
{
    private readonly LoggerInterface $logger;
    private readonly TestHandler $logsHandler;

    protected function setUp(): void
    {
        parent::setUp();

        $this->logsHandler = new TestHandler();
        $this->logger = new Logger('test', [$this->logsHandler]);
    }

    public function testCreateEnvironmentAuthenticator(): void
    {
        $guzzleClientFactory = $this->createGuzzleClientFactory($requestsHistory, []);
        $authenticationFactory = new AuthenticatorFactory(
            new PlainAzureApiClientFactory($guzzleClientFactory),
            $this->logger
        );

        putenv('AZURE_TENANT_ID=foo');
        putenv('AZURE_CLIENT_ID=foo');
        putenv('AZURE_CLIENT_SECRET=foo');

        $authenticator = $authenticationFactory->createAuthenticator();
        self::assertInstanceOf(ClientCredentialsEnvironmentAuthenticator::class, $authenticator);
    }

    public function testCreateManagedCredentialsAuthenticator(): void
    {
        $guzzleClientFactory = $this->createGuzzleClientFactory($requestsHistory, []);
        $authenticationFactory = new AuthenticatorFactory(
            new PlainAzureApiClientFactory($guzzleClientFactory),
            $this->logger
        );

        putenv('AZURE_TENANT_ID=');

        $authenticator = $authenticationFactory->createAuthenticator();
        self::assertInstanceOf(ManagedCredentialsAuthenticator::class, $authenticator);
    }

    public function testManagedCredentialsAuthenticatorIsUsedEventIfNotUsable(): void
    {
        /* Even if the instance metadata is not available, the managed credentials authenticator is
            returned because it's verification is optimized out */
        $guzzleClientFactory = $this->createGuzzleClientFactory($requestsHistory, [
            new Response(400),
        ]);
        $authenticationFactory = new AuthenticatorFactory(
            new PlainAzureApiClientFactory($guzzleClientFactory),
            $this->logger
        );

        putenv('AZURE_TENANT_ID=');

        $authenticator = $authenticationFactory->createAuthenticator();
        self::assertInstanceOf(ManagedCredentialsAuthenticator::class, $authenticator);
        self::assertInstanceOf(ManagedCredentialsAuthenticator::class, $authenticator);
        self::assertTrue($this->logsHandler->hasDebugThatContains(
            'ClientCredentialsEnvironmentAuthenticator is not usable: ' .
            'Environment variable "AZURE_TENANT_ID" is not set.'
        ));
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
