<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Tests;

use GuzzleHttp\Client as GuzzleClient;
use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Middleware;
use GuzzleHttp\Promise\Create;
use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Psr7\Response;
use Keboola\AzureApiClient\ApiClient;
use Keboola\AzureApiClient\Exception\ClientException;
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
    public function testInvalidOptions(array $options, string $expectedError): void
    {
        $this->expectException(ClientException::class);
        $this->expectExceptionMessage($expectedError);

        new ApiClient($options);
    }

    public function provideInvalidOptions(): iterable
    {
        // phpcs:disable Generic.Files.LineLength
        yield 'empty baseUrl' => [
            'options' => [
                'baseUrl' => '',
            ],
            'error' => 'Invalid options when creating client: [baseUrl]: This value is too short. It should have 1 character or more.',
        ];

        yield 'invalid baseUrl' => [
            'options' => [
                'baseUrl' => 'foo',
            ],
            'error' => 'Invalid options when creating client: [baseUrl]: This value is not a valid URL.',
        ];

        yield 'invalid backoffMaxTries' => [
            'options' => [
                'backoffMaxTries' => 'foo',
            ],
            'error' => 'Invalid options when creating client: [backoffMaxTries]: This value should be of type int.',
        ];

        yield 'negative backoffMaxTries' => [
            'options' => [
                'backoffMaxTries' => -1,
            ],
            'error' => 'Invalid options when creating client: [backoffMaxTries]: This value should be greater than or equal to 0.',
        ];

        yield 'invalid middleware' => [
            'options' => [
                'middleware' => 'foo',
            ],
            'error' => 'Invalid options when creating client: [middleware]: This value should be of type iterable.',
        ];

        yield 'invalid middleware item' => [
            'options' => [
                'middleware' => ['foo'],
            ],
            'error' => 'Invalid options when creating client: [middleware][0]: This value should be of type callable.',
        ];

        yield 'invalid requestHandler' => [
            'options' => [
                'requestHandler' => ['foo'],
            ],
            'error' => 'Invalid options when creating client: [requestHandler]: This value should be of type callable.',
        ];

        yield 'invalid logger' => [
            'options' => [
                'logger' => 'foo',
            ],
            'error' => 'Invalid options when creating client: [logger]: This value should be of type Psr\Log\LoggerInterface.',
        ];
        // phpcs:enable Generic.Files.LineLength
    }

    public function testLogger(): void
    {

        $client = new ApiClient([
            'logger' => $this->logger,
            'requestHandler' => fn($request) => Create::promiseFor(new Response(201, [], 'boo')),
        ]);
        $client->sendRequest(new Request('GET', '/'));
        self::assertTrue($this->logsHandler->hasInfoThatMatches(
            '#^[\w\d]+ Keboola Azure PHP Client - \[\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\+00:00\] "GET  /1.1" 201 $#',
        ));
    }
}
