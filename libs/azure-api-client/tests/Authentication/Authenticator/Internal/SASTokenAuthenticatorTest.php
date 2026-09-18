<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Tests\Authentication\Authenticator\Internal;

use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Middleware;
use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Psr7\Response;
use Keboola\AzureApiClient\ApiClientConfiguration;
use Keboola\AzureApiClient\Authentication\Authenticator\Internal\SystemAuthenticatorResolver;
use Monolog\Handler\TestHandler;
use Monolog\Logger;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;

class SASTokenAuthenticatorTest extends TestCase
{
    private readonly LoggerInterface $logger;
    private readonly TestHandler $logsHandler;

    protected function setUp(): void
    {
        parent::setUp();

        $this->logsHandler = new TestHandler();
        $this->logger = new Logger('tests', [$this->logsHandler]);

        putenv('AZURE_TENANT_ID');
        putenv('AZURE_CLIENT_ID');
        putenv('AZURE_CLIENT_SECRET');
    }

    public function testClientCredentialsAuthenticatorIsUsedWhenEnvIsSet(): void
    {
        $requestHandler = self::createRequestHandler($requestsHistory, [
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                (string) file_get_contents(__DIR__ . '/../arm-metadata.json'),
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

        $auth = new SystemAuthenticatorResolver(new ApiClientConfiguration(
            requestHandler: $requestHandler(...),
            logger: $this->logger,
        ));

        putenv('AZURE_TENANT_ID=tenant-id');
        putenv('AZURE_CLIENT_ID=client-id');
        putenv('AZURE_CLIENT_SECRET=client-secret-id');

        $token = $auth->getAuthenticationToken('resource-id');

        self::assertSame('ey....ey', $token->value);
        self::assertEqualsWithDelta(time() + 3599, $token->expiresAt?->getTimestamp(), 1);
        self::assertTrue($this->logsHandler->hasDebug(
            'Found Azure client credentials in ENV, using ClientCredentialsAuthenticator'
        ));
    }

    public function testManagedCredentialsAuthenticatorIsUsedAsFallback(): void
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

        $auth = new SystemAuthenticatorResolver(new ApiClientConfiguration(
            requestHandler: $requestHandler(...),
            logger: $this->logger,
        ));

        $token = $auth->getAuthenticationToken('resource-id');

        self::assertSame('ey....ey', $token->value);
        self::assertEqualsWithDelta(time() + 3599, $token->expiresAt?->getTimestamp(), 1);
        self::assertTrue($this->logsHandler->hasDebug(
            'Azure client credentials not found in ENV, using ManagedCredentialsAuthenticator'
        ));
    }

    /**
     * @param list<array{request: Request, response: Response}> $requestsHistory
     * @param Response[]                                        $responses
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
