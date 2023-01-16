<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Tests\Authentication;

use GuzzleHttp\Client;
use GuzzleHttp\Exception\ClientException as GuzzleClientException;
use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Middleware;
use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Psr7\Response;
use Keboola\AzureApiClient\Authentication\AuthenticatorFactory;
use Keboola\AzureApiClient\Authentication\ClientCredentialsEnvironmentAuthenticator;
use Keboola\AzureApiClient\Authentication\ManagedCredentialsAuthenticator;
use Keboola\AzureApiClient\GuzzleClientFactory;
use Keboola\AzureApiClient\Tests\BaseTest;
use Monolog\Handler\TestHandler;
use Monolog\Logger;
use Psr\Log\NullLogger;

class AuthenticationFactoryTest extends BaseTest
{
    public function testValidClientEnvironmentSettings(): void
    {
        $authenticationFactory = new AuthenticatorFactory();
        $authenticator = $authenticationFactory->getAuthenticator(new GuzzleClientFactory(new NullLogger()));
        self::assertInstanceOf(ClientCredentialsEnvironmentAuthenticator::class, $authenticator);
    }

    public function testInvalidMetadataSettings(): void
    {
        /* Even if the instance metadata is not available, the managed credentials authenticator is
            returned because it's verification is optimized out */
        $logsHandler = new TestHandler();
        $logger = new Logger('tests', [$logsHandler]);

        $mock = $this->createMock(Client::class);
        $mock->method('get')
            ->with('/metadata?api-version=2019-11-01&format=text')
            ->willThrowException(new GuzzleClientException(
                'boo',
                new Request('GET', '/foo/'),
                new Response()
            ));
        $factoryMock = $this->createMock(GuzzleClientFactory::class);
        $factoryMock->method('getClient')->willReturn($mock);
        $factoryMock->method('getLogger')->willReturn($logger);

        putenv('AZURE_TENANT_ID=');
        $authenticationFactory = new AuthenticatorFactory();
        $authenticator = $authenticationFactory->getAuthenticator($factoryMock);
        self::assertInstanceOf(ManagedCredentialsAuthenticator::class, $authenticator);
        self::assertTrue($logsHandler->hasDebugThatContains(
            'ClientCredentialsEnvironmentAuthenticator is not usable: ' .
            'Environment variable "AZURE_TENANT_ID" is not set.'
        ));
    }

    public function testValidManagedSettings(): void
    {
        putenv('AZURE_TENANT_ID=');
        $mock = new MockHandler([new Response(200, [], '')]);
        $requestHistory = [];
        $history = Middleware::history($requestHistory);
        $stack = HandlerStack::create($mock);
        $stack->push($history);
        $factory = new GuzzleClientFactory(new NullLogger());
        $client = $factory->getClient('https://example.com', ['handler' => $stack]);

        $factory = $this->createMock(GuzzleClientFactory::class);
        $factory->method('getClient')->willReturn($client);
        $authenticationFactory = new AuthenticatorFactory();
        $authenticator = $authenticationFactory->getAuthenticator($factory);
        self::assertInstanceOf(ManagedCredentialsAuthenticator::class, $authenticator);
        self::assertCount(0, $requestHistory);
    }
}
