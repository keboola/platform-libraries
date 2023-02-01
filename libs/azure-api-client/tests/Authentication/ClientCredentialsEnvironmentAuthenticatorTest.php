<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Tests\Authentication;

use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Middleware;
use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Psr7\Response;
use Keboola\AzureApiClient\Authentication\ClientCredentialsEnvironmentAuthenticator;
use Keboola\AzureApiClient\Exception\ClientException;
use Keboola\AzureApiClient\GuzzleClientFactory;
use Keboola\AzureApiClient\Tests\BaseTest;
use Monolog\Handler\TestHandler;
use Monolog\Logger;
use Psr\Log\NullLogger;

class ClientCredentialsEnvironmentAuthenticatorTest extends BaseTest
{
    public function testCheckUsabilityFailureMissingTenant(): void
    {
        $authenticator = new ClientCredentialsEnvironmentAuthenticator(new GuzzleClientFactory(new NullLogger()));
        putenv('AZURE_TENANT_ID=');
        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('Environment variable "AZURE_TENANT_ID" is not set.');
        $authenticator->checkUsability();
    }

    public function testCheckUsabilityFailureMissingClient(): void
    {
        $authenticator = new ClientCredentialsEnvironmentAuthenticator(new GuzzleClientFactory(new NullLogger()));
        putenv('AZURE_CLIENT_ID=');
        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('Environment variable "AZURE_CLIENT_ID" is not set.');
        $authenticator->checkUsability();
    }

    public function testCheckUsabilityFailureMissingSecret(): void
    {
        $authenticator = new ClientCredentialsEnvironmentAuthenticator(new GuzzleClientFactory(new NullLogger()));
        putenv('AZURE_CLIENT_SECRET=');
        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('Environment variable "AZURE_CLIENT_SECRET" is not set.');
        $authenticator->checkUsability();
    }

    public function testValidEnvironmentSettings(): void
    {
        $logsHandler = new TestHandler();
        $logger = new Logger('tests', [$logsHandler]);

        $authenticator = new ClientCredentialsEnvironmentAuthenticator(new GuzzleClientFactory($logger));
        $authenticator->checkUsability();

        self::assertTrue($logsHandler->hasDebugThatContains(
            'AZURE_AD_RESOURCE environment variable is not specified, falling back to default.'
        ));
        self::assertTrue($logsHandler->hasDebugThatContains(
            'AZURE_ENVIRONMENT environment variable is not specified, falling back to default.'
        ));
    }

    public function testValidFullEnvironmentSettings(): void
    {
        putenv('AZURE_AD_RESOURCE=https://example.com');
        putenv('AZURE_ENVIRONMENT=123');

        $logsHandler = new TestHandler();
        $logger = new Logger('tests', [$logsHandler]);

        $authenticator = new ClientCredentialsEnvironmentAuthenticator(new GuzzleClientFactory($logger));
        $authenticator->checkUsability();

        self::assertFalse($logsHandler->hasDebugThatContains(
            'AZURE_AD_RESOURCE environment variable is not specified, falling back to default.'
        ));
        self::assertFalse($logsHandler->hasDebugThatContains(
            'AZURE_ENVIRONMENT environment variable is not specified, falling back to default.'
        ));
    }

    public function testInvalidAdResource(): void
    {
        putenv('AZURE_AD_RESOURCE=not-an-url');
        putenv('AzureCloud=123');

        $logsHandler = new TestHandler();
        $logger = new Logger('tests', [$logsHandler]);

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage(
            'Invalid options when creating client: Value "not-an-url" is invalid: This value is not a valid URL.'
        );
        new ClientCredentialsEnvironmentAuthenticator(new GuzzleClientFactory($logger));
    }

    public function testAuthenticate(): void
    {
        $mock = new MockHandler($this->getMockAuthResponses());
        $requestHistory = [];
        $history = Middleware::history($requestHistory);
        $stack = HandlerStack::create($mock);
        $stack->push($history);

        $factory = new GuzzleClientFactory(new NullLogger());
        $client = $factory->getClient('https://example.com', ['handler' => $stack]);

        $factory = $this->createMock(GuzzleClientFactory::class);
        $factory->method('getClient')->willReturn($client);

        /** @var GuzzleClientFactory $factory */
        $auth = new ClientCredentialsEnvironmentAuthenticator($factory);
        $token = $auth->getAuthenticationToken('resource-id');
        self::assertCount(2, $requestHistory);

        // call second time, value is cached and no new request are made
        $token2 = $auth->getAuthenticationToken('resource-id');
        self::assertCount(2, $requestHistory);
        self::assertSame($token, $token2);
        self::assertEquals('ey....ey', $token);
        /** @var Request $request */
        $request = $requestHistory[0]['request'];
        self::assertEquals(
            'https://management.azure.com/metadata/endpoints?api-version=2020-01-01',
            $request->getUri()->__toString()
        );
        self::assertEquals('GET', $request->getMethod());
        self::assertEquals('Azure PHP Client', $request->getHeader('User-Agent')[0]);
        self::assertEquals('application/json', $request->getHeader('Content-type')[0]);
        /** @var Request $request */
        $request = $requestHistory[1]['request'];
        self::assertEquals('https://login.windows.net/tenant123/oauth2/token', $request->getUri()->__toString());
        self::assertEquals('POST', $request->getMethod());
        self::assertEquals('Azure PHP Client', $request->getHeader('User-Agent')[0]);
        self::assertEquals('application/x-www-form-urlencoded', $request->getHeader('Content-type')[0]);
        self::assertEquals(
            // phpcs:ignore Generic.Files.LineLength
            'grant_type=client_credentials&client_id=client123&client_secret=secret123&resource=resource-id',
            $request->getBody()->getContents()
        );
    }

    public function testAuthenticateCustomMetadata(): void
    {
        $metadata = $this->getSampleArmMetadata();
        $metadata[0]['authentication']['loginEndpoint'] = 'https://my-custom-login/';
        $metadata[0]['name'] = 'my-azure';
        putenv('AZURE_ENVIRONMENT=my-azure');
        putenv('AZURE_AD_RESOURCE=https://example.com');

        $mock = new MockHandler([
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                (string) json_encode($metadata)
            ),
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                '{
                    "token_type": "Bearer",
                    "expires_in": "3599",
                    "ext_expires_in": "3599",
                    "expires_on": "1589810452",
                    "not_before": "1589806552",
                    "resource": "https://vault.azure.net",
                    "access_token": "ey....ey"
                }'
            ),
        ]);

        $requestHistory = [];
        $history = Middleware::history($requestHistory);
        $stack = HandlerStack::create($mock);
        $stack->push($history);
        $factory = new GuzzleClientFactory(new NullLogger());
        $client = $factory->getClient('https://example.com', ['handler' => $stack]);

        $factory = $this->createMock(GuzzleClientFactory::class);
        $factory->method('getClient')->willReturn($client);
        /** @var GuzzleClientFactory $factory */
        $auth = new ClientCredentialsEnvironmentAuthenticator($factory);
        $token = $auth->getAuthenticationToken('resource-id');
        self::assertEquals('ey....ey', $token);
        self::assertCount(2, $requestHistory);
        /** @var Request $request */
        $request = $requestHistory[0]['request'];
        self::assertEquals('https://example.com', $request->getUri()->__toString());
        self::assertEquals('GET', $request->getMethod());
        self::assertEquals('Azure PHP Client', $request->getHeader('User-Agent')[0]);
        self::assertEquals('application/json', $request->getHeader('Content-type')[0]);
        $request = $requestHistory[1]['request'];
        self::assertEquals('https://my-custom-login/tenant123/oauth2/token', $request->getUri()->__toString());
        self::assertEquals('POST', $request->getMethod());
        self::assertEquals('Azure PHP Client', $request->getHeader('User-Agent')[0]);
        self::assertEquals('application/x-www-form-urlencoded', $request->getHeader('Content-type')[0]);
        self::assertEquals(
            // phpcs:ignore Generic.Files.LineLength
            'grant_type=client_credentials&client_id=client123&client_secret=secret123&resource=resource-id',
            $request->getBody()->getContents()
        );
    }

    public function testAuthenticateInvalidMetadata(): void
    {
        putenv('AZURE_ENVIRONMENT=non-existent');
        $mock = new MockHandler([
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                (string) json_encode($this->getSampleArmMetadata())
            ),
        ]);

        $requestHistory = [];
        $history = Middleware::history($requestHistory);
        $stack = HandlerStack::create($mock);
        $stack->push($history);
        $factory = new GuzzleClientFactory(new NullLogger());
        $client = $factory->getClient('https://example.com', ['handler' => $stack]);

        $factory = $this->createMock(GuzzleClientFactory::class);
        $factory->method('getClient')->willReturn($client);

        $auth = new ClientCredentialsEnvironmentAuthenticator($factory);
        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('Cloud "non-existent" not found in instance metadata');
        $auth->getAuthenticationToken('resource-id');
    }

    public function testAuthenticateMetadataRetry(): void
    {
        $mock = new MockHandler([
            new Response(
                500,
                ['Content-Type' => 'application/json'],
                'boo'
            ),
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                (string) json_encode($this->getSampleArmMetadata())
            ),
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                '{
                    "token_type": "Bearer",
                    "expires_in": "3599",
                    "ext_expires_in": "3599",
                    "expires_on": "1589810452",
                    "not_before": "1589806552",
                    "resource": "https://vault.azure.net",
                    "access_token": "ey....ey"
                }'
            ),
        ]);

        $requestHistory = [];
        $history = Middleware::history($requestHistory);
        $stack = HandlerStack::create($mock);
        $stack->push($history);
        $factory = new GuzzleClientFactory(new NullLogger());
        $client = $factory->getClient('https://example.com', ['handler' => $stack]);

        $factory = $this->createMock(GuzzleClientFactory::class);
        $factory->method('getClient')->willReturn($client);
        $auth = new ClientCredentialsEnvironmentAuthenticator($factory);
        $token = $auth->getAuthenticationToken('resource-id');
        self::assertEquals('ey....ey', $token);
    }

    public function testAuthenticateMetadataFailure(): void
    {
        $mock = new MockHandler([
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                'boo'
            ),
        ]);

        $requestHistory = [];
        $history = Middleware::history($requestHistory);
        $stack = HandlerStack::create($mock);
        $stack->push($history);
        $factory = new GuzzleClientFactory(new NullLogger());
        $client = $factory->getClient('https://example.com', ['handler' => $stack]);

        $factory = $this->createMock(GuzzleClientFactory::class);
        $factory->method('getClient')->willReturn($client);
        $auth = new ClientCredentialsEnvironmentAuthenticator($factory);

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('Failed to get instance metadata: Syntax error');
        $auth->getAuthenticationToken('resource-id');
    }

    public function testAuthenticateMalformedMetadata(): void
    {
        $mock = new MockHandler([
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                '"boo"'
            ),
        ]);

        $requestHistory = [];
        $history = Middleware::history($requestHistory);
        $stack = HandlerStack::create($mock);
        $stack->push($history);
        $factory = new GuzzleClientFactory(new NullLogger());
        $client = $factory->getClient('https://example.com', ['handler' => $stack]);

        $factory = $this->createMock(GuzzleClientFactory::class);
        $factory->method('getClient')->willReturn($client);
        $auth = new ClientCredentialsEnvironmentAuthenticator($factory);

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('Invalid metadata contents: "boo"');
        $auth->getAuthenticationToken('resource-id');
    }

    public function testAuthenticateTokenError(): void
    {
        $mock = new MockHandler([
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                (string) json_encode($this->getSampleArmMetadata())
            ),
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                '{"boo"}'
            ),
        ]);

        $requestHistory = [];
        $history = Middleware::history($requestHistory);
        $stack = HandlerStack::create($mock);
        $stack->push($history);
        $factory = new GuzzleClientFactory(new NullLogger());
        $client = $factory->getClient('https://example.com', ['handler' => $stack]);

        $factory = $this->createMock(GuzzleClientFactory::class);
        $factory->method('getClient')->willReturn($client);
        $auth = new ClientCredentialsEnvironmentAuthenticator($factory);

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('Failed to get authentication token: Syntax error');
        $auth->getAuthenticationToken('resource-id');
    }

    public function testAuthenticateTokenMalformed(): void
    {
        $mock = new MockHandler([
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                (string) json_encode($this->getSampleArmMetadata())
            ),
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                '{"error": "boo"}'
            ),
        ]);

        $requestHistory = [];
        $history = Middleware::history($requestHistory);
        $stack = HandlerStack::create($mock);
        $stack->push($history);
        $factory = new GuzzleClientFactory(new NullLogger());
        $client = $factory->getClient('https://example.com', ['handler' => $stack]);

        $factory = $this->createMock(GuzzleClientFactory::class);
        $factory->method('getClient')->willReturn($client);
        $auth = new ClientCredentialsEnvironmentAuthenticator($factory);

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('Access token not provided in response: {"error":"boo"}');
        $auth->getAuthenticationToken('resource-id');
    }
}
