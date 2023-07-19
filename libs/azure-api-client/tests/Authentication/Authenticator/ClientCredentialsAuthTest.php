<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Tests\Authentication\Authenticator;

use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Middleware;
use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Psr7\Response;
use Keboola\AzureApiClient\ApiClientConfiguration;
use Keboola\AzureApiClient\Authentication\Authenticator\ClientCredentialsAuth;
use Keboola\AzureApiClient\Exception\ClientException;
use Keboola\AzureApiClient\Json;
use Monolog\Handler\TestHandler;
use Monolog\Logger;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;

/**
 * @runTestsInSeparateProcesses because of env variables
 */
class ClientCredentialsAuthTest extends TestCase
{
    private readonly LoggerInterface $logger;
    private readonly TestHandler $logsHandler;

    public function setUp(): void
    {
        parent::setUp();

        $this->logsHandler = new TestHandler();
        $this->logger = new Logger('tests', [$this->logsHandler]);
    }

    public function testOptionalEnvsFallback(): void
    {
        putenv('AZURE_AD_RESOURCE=https://foo');
        putenv('AZURE_ENVIRONMENT=foo');
        new ClientCredentialsAuth(
            'tenant-id',
            'client-id',
            'client-secret',
            new ApiClientConfiguration(
                logger: $this->logger,
            )
        );
        self::assertCount(0, $this->logsHandler->getRecords());

        putenv('AZURE_AD_RESOURCE');
        putenv('AZURE_ENVIRONMENT');
        new ClientCredentialsAuth(
            'tenant-id',
            'client-id',
            'client-secret',
            new ApiClientConfiguration(
                logger: $this->logger,
            )
        );
        self::assertTrue($this->logsHandler->hasDebug(
            'AZURE_AD_RESOURCE environment variable is not specified, falling back to default.'
        ));
        self::assertTrue($this->logsHandler->hasDebug(
            'AZURE_ENVIRONMENT environment variable is not specified, falling back to default.'
        ));
    }

    public function testGetAuthenticationToken(): void
    {
        $metadata = $this->getSampleArmMetadata();

        $requestHandler = self::createRequestHandler($requestsHistory, [
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

        $auth = new ClientCredentialsAuth(
            'tenant-id',
            'client-id',
            'client-secret',
            new ApiClientConfiguration(
                requestHandler: $requestHandler(...),
                logger: $this->logger,
            )
        );

        $token = $auth->getAuthenticationToken('resource-id');
        self::assertCount(2, $requestsHistory);
        self::assertSame('ey....ey', $token->value);
        self::assertEqualsWithDelta(time() + 3599, $token->expiresAt?->getTimestamp(), 1);

        $request = $requestsHistory[0]['request'];
        self::assertSame(
            'https://management.azure.com/metadata/endpoints?api-version=2020-01-01',
            $request->getUri()->__toString()
        );
        self::assertSame('GET', $request->getMethod());

        $request = $requestsHistory[1]['request'];
        self::assertSame('https://login.windows.net/tenant-id/oauth2/token', $request->getUri()->__toString());
        self::assertSame('POST', $request->getMethod());
        self::assertSame('application/x-www-form-urlencoded', $request->getHeader('Content-type')[0]);
        self::assertSame(
            'grant_type=client_credentials&client_id=client-id&client_secret=client-secret&resource=resource-id',
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

        $requestHandler = self::createRequestHandler($requestsHistory, [
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

        $auth = new ClientCredentialsAuth(
            'tenant-id',
            'client-id',
            'client-secret',
            new ApiClientConfiguration(
                requestHandler: $requestHandler(...),
                logger: $this->logger,
            )
        );

        $token = $auth->getAuthenticationToken('resource-id');
        self::assertCount(2, $requestsHistory);
        self::assertSame('ey....ey', $token->value);
        self::assertEqualsWithDelta(time() + 3599, $token->expiresAt?->getTimestamp(), 1);

        $request = $requestsHistory[0]['request'];
        self::assertSame(
            'https://example.com',
            $request->getUri()->__toString()
        );
        self::assertSame('GET', $request->getMethod());

        $request = $requestsHistory[1]['request'];
        self::assertSame('https://my-custom-login/tenant-id/oauth2/token', $request->getUri()->__toString());
        self::assertSame('POST', $request->getMethod());
        self::assertSame('application/x-www-form-urlencoded', $request->getHeader('Content-type')[0]);
        self::assertSame(
            'grant_type=client_credentials&client_id=client-id&client_secret=client-secret&resource=resource-id',
            $request->getBody()->getContents()
        );
    }

    public function testGetAuthenticationTokenWithInvalidCustomMetadata(): void
    {
        putenv('AZURE_ENVIRONMENT=non-existent');

        $requestHandler = self::createRequestHandler($requestsHistory, [
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                Json::encodeArray($this->getSampleArmMetadata())
            ),
        ]);

        $auth = new ClientCredentialsAuth(
            'tenant-id',
            'client-id',
            'client-secret',
            new ApiClientConfiguration(
                requestHandler: $requestHandler(...),
                logger: $this->logger,
            )
        );

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('Cloud "non-existent" not found in instance metadata');

        $auth->getAuthenticationToken('resource-id');
    }

    public function testGetAuthenticationTokenMetadataRetry(): void
    {
        $requestHandler = self::createRequestHandler($requestsHistory, [
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

        $auth = new ClientCredentialsAuth(
            'tenant-id',
            'client-id',
            'client-secret',
            new ApiClientConfiguration(
                requestHandler: $requestHandler(...),
                logger: $this->logger,
            )
        );
        $token = $auth->getAuthenticationToken('resource-id');
        self::assertEquals('ey....ey', $token->value);
        self::assertEqualsWithDelta(time() + 3599, $token->expiresAt?->getTimestamp(), 1);
    }

    public function testGetAuthenticationTokenMetadataFailure(): void
    {
        $requestHandler = self::createRequestHandler($requestsHistory, [
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                'boo'
            ),
        ]);

        $auth = new ClientCredentialsAuth(
            'tenant-id',
            'client-id',
            'client-secret',
            new ApiClientConfiguration(
                requestHandler: $requestHandler(...),
                logger: $this->logger,
            )
        );

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('Failed to get instance metadata: Response is not a valid JSON: Syntax error');
        $auth->getAuthenticationToken('resource-id');
    }

    public function testGetAuthenticationTokenTokenError(): void
    {
        $requestHandler = self::createRequestHandler($requestsHistory, [
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

        $auth = new ClientCredentialsAuth(
            'tenant-id',
            'client-id',
            'client-secret',
            new ApiClientConfiguration(
                requestHandler: $requestHandler(...),
                logger: $this->logger,
            )
        );

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage(
            'Failed to map response data: Missing or invalid "access_token" in response: {"boo":"bar"}'
        );
        $auth->getAuthenticationToken('resource-id');
    }

    /**
     * @param list<array{request: Request, response: Response}> $requestsHistory
     * @param array                                             $responses
     * @return HandlerStack
     */
    private static function createRequestHandler(?array &$requestsHistory, array $responses): HandlerStack
    {
        $requestsHistory = [];

        $stack = HandlerStack::create(new MockHandler($responses));
        $stack->push(Middleware::history($requestsHistory));

        return $stack;
    }

    private function getSampleArmMetadata(): array
    {
        return Json::decodeArray((string) file_get_contents(__DIR__.'/arm-metadata.json'));
    }
}
