<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Tests\Authentication;

use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Middleware;
use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Psr7\Response;
use Keboola\AzureApiClient\ApiClientFactory\PlainAzureApiClientFactory;
use Keboola\AzureApiClient\Authentication\ClientCredentialsEnvironmentAuthenticator;
use Keboola\AzureApiClient\Exception\ClientException;
use Keboola\AzureApiClient\GuzzleClientFactory;
use Keboola\AzureApiClient\Json;
use Keboola\AzureApiClient\Tests\BaseTest;
use Monolog\Handler\TestHandler;
use Monolog\Logger;
use Psr\Log\LoggerInterface;

class ClientCredentialsEnvironmentAuthenticatorTest extends BaseTest
{
    private readonly PlainAzureApiClientFactory $clientFactory;
    private readonly LoggerInterface $logger;
    private readonly TestHandler $logsHandler;

    public function setUp(): void
    {
        parent::setUp();

        $this->logsHandler = new TestHandler();
        $this->logger = new Logger('tests', [$this->logsHandler]);

        $this->clientFactory = new PlainAzureApiClientFactory(
            new GuzzleClientFactory($this->logger),
        );
    }

    public function testOptionalEnvsFallback(): void
    {
        putenv('AZURE_AD_RESOURCE=http://foo');
        putenv('AZURE_ENVIRONMENT=foo');
        new ClientCredentialsEnvironmentAuthenticator($this->clientFactory, $this->logger);
        self::assertCount(0, $this->logsHandler->getRecords());

        putenv('AZURE_AD_RESOURCE');
        putenv('AZURE_ENVIRONMENT');
        new ClientCredentialsEnvironmentAuthenticator($this->clientFactory, $this->logger);
        self::assertTrue($this->logsHandler->hasDebug(
            'AZURE_AD_RESOURCE environment variable is not specified, falling back to default.'
        ));
        self::assertTrue($this->logsHandler->hasDebug(
            'AZURE_ENVIRONMENT environment variable is not specified, falling back to default.'
        ));
    }

    public function testCheckUsabilitySuccess(): void
    {
        putenv('AZURE_TENANT_ID=foo');
        putenv('AZURE_CLIENT_ID=foo');
        putenv('AZURE_CLIENT_SECRET=foo');

        $authenticator = new ClientCredentialsEnvironmentAuthenticator($this->clientFactory, $this->logger);
        $authenticator->checkUsability();

        self::expectNotToPerformAssertions();
    }

    /** @dataProvider provideCheckUsabilityFailureTestData */
    public function testCheckUsabilityFailure(string $requiredEnv): void
    {
        putenv($requiredEnv);

        $authenticator = new ClientCredentialsEnvironmentAuthenticator($this->clientFactory, $this->logger);

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage(sprintf('Environment variable "%s" is not set.', $requiredEnv));

        $authenticator->checkUsability();
    }

    public function provideCheckUsabilityFailureTestData(): iterable
    {
        yield 'AZURE_TENANT_ID' => ['AZURE_TENANT_ID'];
        yield 'AZURE_CLIENT_ID' => ['AZURE_CLIENT_ID'];
        yield 'AZURE_CLIENT_SECRET' => ['AZURE_CLIENT_SECRET'];
    }

    public function testGetAuthenticationToken(): void
    {
        $metadata = $this->getSampleArmMetadata();

        $requestHandler = self::prepareGuzzleMockHandler($requestsHistory, [
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                Json::encodeArray($metadata)
            ),
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

        $guzzleClientFactory = new GuzzleClientFactory($this->logger, $requestHandler);
        $apiClientFactory = new PlainAzureApiClientFactory($guzzleClientFactory);

        $auth = new ClientCredentialsEnvironmentAuthenticator(
            $apiClientFactory,
            $this->logger
        );

        $token = $auth->getAuthenticationToken('resource-id');
        self::assertCount(2, $requestsHistory);
        self::assertSame('ey....ey', $token->accessToken);
        self::assertEqualsWithDelta(time() + 3599, $token->accessTokenExpiration->getTimestamp(), 1);

        $request = $requestsHistory[0]['request'];
        self::assertSame(
            'https://management.azure.com/metadata/endpoints?api-version=2020-01-01',
            $request->getUri()->__toString()
        );
        self::assertSame('GET', $request->getMethod());
        self::assertSame('application/json', $request->getHeader('Content-type')[0]);

        $request = $requestsHistory[1]['request'];
        self::assertSame('https://login.windows.net/tenant123/oauth2/token', $request->getUri()->__toString());
        self::assertSame('POST', $request->getMethod());
        self::assertSame('application/x-www-form-urlencoded', $request->getHeader('Content-type')[0]);
        self::assertSame(
            'grant_type=client_credentials&client_id=client123&client_secret=secret123&resource=resource-id',
            $request->getBody()->getContents()
        );
    }


    public function testGetAuthenticationTokenWithCustomMetadata(): void
    {
        $metadata = $this->getSampleArmMetadata();
        $metadata[0]['authentication']['loginEndpoint'] = 'https://my-custom-login/';
        $metadata[0]['name'] = 'my-azure';
        putenv('AZURE_ENVIRONMENT=my-azure');
        putenv('AZURE_AD_RESOURCE=https://example.com');

        $requestHandler = self::prepareGuzzleMockHandler($requestsHistory, [
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                Json::encodeArray($metadata)
            ),
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

        $guzzleClientFactory = new GuzzleClientFactory($this->logger, $requestHandler);
        $apiClientFactory = new PlainAzureApiClientFactory($guzzleClientFactory);

        $auth = new ClientCredentialsEnvironmentAuthenticator(
            $apiClientFactory,
            $this->logger
        );

        $token = $auth->getAuthenticationToken('resource-id');
        self::assertCount(2, $requestsHistory);
        self::assertSame('ey....ey', $token->accessToken);
        self::assertEqualsWithDelta(time() + 3599, $token->accessTokenExpiration->getTimestamp(), 1);

        $request = $requestsHistory[0]['request'];
        self::assertSame(
            'https://example.com',
            $request->getUri()->__toString()
        );
        self::assertSame('GET', $request->getMethod());
        self::assertSame('application/json', $request->getHeader('Content-type')[0]);

        $request = $requestsHistory[1]['request'];
        self::assertSame('https://my-custom-login/tenant123/oauth2/token', $request->getUri()->__toString());
        self::assertSame('POST', $request->getMethod());
        self::assertSame('application/x-www-form-urlencoded', $request->getHeader('Content-type')[0]);
        self::assertSame(
            'grant_type=client_credentials&client_id=client123&client_secret=secret123&resource=resource-id',
            $request->getBody()->getContents()
        );
    }

    public function testGetAuthenticationTokenWithInvalidCustomMetadata(): void
    {
        putenv('AZURE_ENVIRONMENT=non-existent');

        $requestHandler = self::prepareGuzzleMockHandler($requestsHistory, [
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                Json::encodeArray($this->getSampleArmMetadata())
            ),
        ]);

        $guzzleClientFactory = new GuzzleClientFactory($this->logger, $requestHandler);
        $apiClientFactory = new PlainAzureApiClientFactory($guzzleClientFactory);

        $auth = new ClientCredentialsEnvironmentAuthenticator(
            $apiClientFactory,
            $this->logger
        );

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('Cloud "non-existent" not found in instance metadata');

        $auth->getAuthenticationToken('resource-id');
    }

    public function testGetAuthenticationTokenMetadataRetry(): void
    {
        $requestHandler = self::prepareGuzzleMockHandler($requestsHistory, [
            new Response(
                500,
                ['Content-Type' => 'application/json'],
                'boo'
            ),
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                Json::encodeArray($this->getSampleArmMetadata())
            ),
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

        $guzzleClientFactory = new GuzzleClientFactory($this->logger, $requestHandler);
        $apiClientFactory = new PlainAzureApiClientFactory($guzzleClientFactory);

        $auth = new ClientCredentialsEnvironmentAuthenticator(
            $apiClientFactory,
            $this->logger
        );

        $token = $auth->getAuthenticationToken('resource-id');
        self::assertEquals('ey....ey', $token->accessToken);
        self::assertEqualsWithDelta(time() + 3599, $token->accessTokenExpiration->getTimestamp(), 1);
    }

    public function testGetAuthenticationTokenMetadataFailure(): void
    {
        $requestHandler = self::prepareGuzzleMockHandler($requestsHistory, [
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                'boo'
            ),
        ]);

        $guzzleClientFactory = new GuzzleClientFactory($this->logger, $requestHandler);
        $apiClientFactory = new PlainAzureApiClientFactory($guzzleClientFactory);

        $auth = new ClientCredentialsEnvironmentAuthenticator(
            $apiClientFactory,
            $this->logger
        );

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('Failed to get instance metadata: Response is not a valid JSON: Syntax error');
        $auth->getAuthenticationToken('resource-id');
    }

    public function testGetAuthenticationTokenTokenError(): void
    {
        $requestHandler = self::prepareGuzzleMockHandler($requestsHistory, [
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                Json::encodeArray($this->getSampleArmMetadata())
            ),
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                '{"boo":"bar"}'
            ),
        ]);

        $guzzleClientFactory = new GuzzleClientFactory($this->logger, $requestHandler);
        $apiClientFactory = new PlainAzureApiClientFactory($guzzleClientFactory);

        $auth = new ClientCredentialsEnvironmentAuthenticator(
            $apiClientFactory,
            $this->logger
        );

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage(
            'Failed to map response data: Missing or invalid "access_token" in response: {"boo":"bar"}'
        );
        $auth->getAuthenticationToken('resource-id');
    }

    /**
     * @param list<array{request: Request, response: Response}> $requestsHistory
     * @param list<Response>                                    $responses
     * @return HandlerStack
     */
    private static function prepareGuzzleMockHandler(?array &$requestsHistory, array $responses): HandlerStack
    {
        $requestsHistory = [];

        $stack = HandlerStack::create(new MockHandler($responses));
        $stack->push(Middleware::history($requestsHistory));

        return $stack;
    }
}
