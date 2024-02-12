<?php

declare(strict_types=1);

namespace Keboola\SandboxesServiceApiClient\Tests;

use GuzzleHttp\Client as GuzzleClient;
use GuzzleHttp\Promise\Create;
use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Psr7\Response;
use InvalidArgumentException;
use Keboola\SandboxesServiceApiClient\ApiClient;
use Keboola\SandboxesServiceApiClient\ApiClientConfiguration;
use Monolog\Handler\TestHandler;
use Monolog\Logger;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;

class ApiClientTest extends TestCase
{
    use ReflectionPropertyAccessTestCase;

    private readonly LoggerInterface $logger;
    private readonly TestHandler $logsHandler;

    protected function setUp(): void
    {
        parent::setUp();

        $this->logsHandler = new TestHandler();
        $this->logger = new Logger('tests', [$this->logsHandler]);
    }

    public function testCreateClientWithDefaults(): void
    {
        $client = new ApiClient();

        $httpClient = self::getPrivatePropertyValue($client, 'httpClient');
        self::assertInstanceOf(GuzzleClient::class, $httpClient);
        $httpClientConfig = self::getPrivatePropertyValue($httpClient, 'config');
        self::assertIsArray($httpClientConfig);

        self::assertNull($httpClientConfig['base_uri']);
        self::assertSame(['User-Agent' => 'Keboola Azure PHP Client'], $httpClientConfig['headers']);
        self::assertSame(120, $httpClientConfig['timeout']);
        self::assertSame(10, $httpClientConfig['connect_timeout']);
    }

    /** @dataProvider provideInvalidOptions */
    public function testInvalidOptions(
        ?string $baseUrl,
        ?int $backoffMaxTries,
        string $expectedError,
    ): void {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage($expectedError);

        new ApiClient(
            $baseUrl, // @phpstan-ignore-line intentionally passing invalid values
            new ApiClientConfiguration(
                backoffMaxTries: $backoffMaxTries, // @phpstan-ignore-line
            ),
        );
    }

    public function provideInvalidOptions(): iterable
    {
        yield 'empty baseUrl' => [
            'baseUrl' => '',
            'backoffMaxTries' => 0,
            'error' => 'Expected a value to contain at least 1 characters. Got: ""',
        ];

        yield 'negative backoffMaxTries' => [
            'baseUrl' => null,
            'backoffMaxTries' => -1,
            'error' => 'Expected a value greater than or equal to 0. Got: -1',
        ];
    }

    public function testLogger(): void
    {

        $client = new ApiClient(
            configuration: new ApiClientConfiguration(
                requestHandler: fn($request) => Create::promiseFor(new Response(201, [], 'boo')),
                logger: $this->logger,
            ),
        );
        $client->sendRequest(new Request('GET', '/'));
        self::assertTrue($this->logsHandler->hasInfoThatMatches(
            '#^[\w\d]+ Keboola Azure PHP Client - \[\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\+00:00\] "GET  /1.1" 201 $#',
        ));
    }
}
