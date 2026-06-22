<?php

declare(strict_types=1);

namespace Keboola\VaultApiClient\Tests\Variables;

use GuzzleHttp\Psr7\Response;
use InvalidArgumentException;
use Keboola\ApiClientBase\Json;
use Keboola\VaultApiClient\Exception\VaultClientException;
use Keboola\VaultApiClient\Tests\ApiClientTestTrait;
use Keboola\VaultApiClient\Variables\Model\ListOptions;
use Keboola\VaultApiClient\Variables\Model\Variable;
use Keboola\VaultApiClient\Variables\VariablesApiClient;
use Monolog\Handler\TestHandler;
use Monolog\Logger;
use PHPUnit\Framework\TestCase;
use ReflectionMethod;

class VariablesApiClientTest extends TestCase
{
    use ApiClientTestTrait;

    private const BASE_URL = 'https://vault.keboola.com';
    private const API_TOKEN = 'my-token';

    public function testCreateVariable(): void
    {
        $requestHandler = self::createRequestHandler($requestsHistory, [
            new Response(
                200,
                [
                    'Content-Type' => 'application/json',
                ],
                Json::encodeArray([
                    'hash' => 'hash',
                    'key' => 'key',
                    'value' => 'val',
                    'flags' => ['encrypted'],
                    'attributes' => [
                        'branchId' => '123',
                    ],
                ]),
            ),
        ]);

        $client = new VariablesApiClient(
            self::BASE_URL,
            self::API_TOKEN,
            requestHandler: $requestHandler(...),
        );

        $variable = $client->createVariable(
            'key',
            'val',
            [Variable::FLAG_ENCRYPTED],
            ['branchId' => '123'],
        );

        self::assertCount(1, $requestsHistory);
        self::assertRequestEquals(
            'POST',
            self::BASE_URL . '/variables',
            [
                'Content-Type' => 'application/json',
                'X-StorageApi-Token' => self::API_TOKEN,
            ],
            Json::encodeArray([
                'key' => 'key',
                'value' => 'val',
                'flags' => ['encrypted'],
                'attributes' => [
                    'branchId' => '123',
                ],
            ]),
            $requestsHistory[0]['request'],
        );

        self::assertEquals(
            new Variable(
                hash: 'hash',
                key: 'key',
                value: 'val',
                flags: [Variable::FLAG_ENCRYPTED],
                attributes: [
                    'branchId' => '123',
                ],
            ),
            $variable,
        );
    }

    public function testDeleteVariable(): void
    {
        $requestHandler = self::createRequestHandler($requestsHistory, [
            new Response(200),
        ]);

        $client = new VariablesApiClient(
            self::BASE_URL,
            self::API_TOKEN,
            requestHandler: $requestHandler(...),
        );

        $client->deleteVariable('hash');

        self::assertCount(1, $requestsHistory);
        self::assertRequestEquals(
            'DELETE',
            self::BASE_URL . '/variables/hash',
            [
                'X-StorageApi-Token' => self::API_TOKEN,
            ],
            null,
            $requestsHistory[0]['request'],
        );
    }

    public function testListVariables(): void
    {
        $requestHandler = self::createRequestHandler($requestsHistory, [
            new Response(
                200,
                [
                    'Content-Type' => 'application/json',
                ],
                Json::encodeArray([
                    ['hash' => 'hash1', 'key' => 'key1', 'value' => 'val1', 'flags' => [], 'attributes' => []],
                    ['hash' => 'hash2', 'key' => 'key2', 'value' => 'val2', 'flags' => [], 'attributes' => []],
                ]),
            ),
        ]);

        $client = new VariablesApiClient(
            self::BASE_URL,
            self::API_TOKEN,
            requestHandler: $requestHandler(...),
        );

        $variables = $client->listVariables(new ListOptions(offset: 5));

        self::assertCount(1, $requestsHistory);
        self::assertRequestEquals(
            'GET',
            self::BASE_URL . '/variables?offset=5',
            [
                'X-StorageApi-Token' => self::API_TOKEN,
            ],
            null,
            $requestsHistory[0]['request'],
        );

        self::assertCount(2, $variables);
        self::assertEquals(
            new Variable(
                hash: 'hash1',
                key: 'key1',
                value: 'val1',
                flags: [],
                attributes: [],
            ),
            $variables[0],
        );
        self::assertEquals(
            new Variable(
                hash: 'hash2',
                key: 'key2',
                value: 'val2',
                flags: [],
                attributes: [],
            ),
            $variables[1],
        );
    }

    public function testListScopedVariablesForBranch(): void
    {
        $requestHandler = self::createRequestHandler($requestsHistory, [
            new Response(
                200,
                [
                    'Content-Type' => 'application/json',
                ],
                Json::encodeArray([
                    ['hash' => 'hash1', 'key' => 'key1', 'value' => 'val1', 'flags' => [], 'attributes' => []],
                    ['hash' => 'hash2', 'key' => 'key2', 'value' => 'val2', 'flags' => [], 'attributes' => []],
                ]),
            ),
        ]);

        $client = new VariablesApiClient(
            self::BASE_URL,
            self::API_TOKEN,
            requestHandler: $requestHandler(...),
        );

        $variables = $client->listScopedVariablesForBranch('123');

        self::assertCount(1, $requestsHistory);
        self::assertRequestEquals(
            'GET',
            self::BASE_URL . '/variables/scoped/branch/123',
            [
                'X-StorageApi-Token' => self::API_TOKEN,
            ],
            null,
            $requestsHistory[0]['request'],
        );

        self::assertCount(2, $variables);
        self::assertEquals(
            new Variable(
                hash: 'hash1',
                key: 'key1',
                value: 'val1',
                flags: [],
                attributes: [],
            ),
            $variables[0],
        );
        self::assertEquals(
            new Variable(
                hash: 'hash2',
                key: 'key2',
                value: 'val2',
                flags: [],
                attributes: [],
            ),
            $variables[1],
        );
    }

    public function testEmptyBaseUrlThrows(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Base URL must be a non-empty string');

        new VariablesApiClient('', self::API_TOKEN); // @phpstan-ignore-line
    }

    public function testEmptyTokenThrows(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Storage API token must not be empty');

        new VariablesApiClient(self::BASE_URL, ''); // @phpstan-ignore-line
    }

    public function testClientErrorWithVaultFormat(): void
    {
        $requestHandler = self::createRequestHandler($requestsHistory, [
            new Response(
                400,
                ['Content-Type' => 'application/json'],
                Json::encodeArray([
                    'error' => 'Missing data',
                    'code' => 400,
                ]),
            ),
        ]);

        $client = new VariablesApiClient(
            self::BASE_URL,
            self::API_TOKEN,
            backoffMaxTries: 0,
            requestHandler: $requestHandler(...),
        );

        $this->expectException(VaultClientException::class);
        $this->expectExceptionMessage('400: Missing data');

        $client->deleteVariable('hash');
    }

    public function testClientErrorWithNonStandardFormat(): void
    {
        $requestHandler = self::createRequestHandler($requestsHistory, [
            new Response(
                400,
                ['Content-Type' => 'application/json'],
                Json::encodeArray([
                    'error' => 'Missing data',
                ]),
            ),
        ]);

        $client = new VariablesApiClient(
            self::BASE_URL,
            self::API_TOKEN,
            backoffMaxTries: 0,
            requestHandler: $requestHandler(...),
        );

        $this->expectException(VaultClientException::class);
        $this->expectExceptionMessage(
            'Client error: `DELETE https://vault.keboola.com/variables/hash` resulted in a `400 Bad Request` response',
        );

        $client->deleteVariable('hash');
    }

    public function testTokenIsSetAsHeader(): void
    {
        $requestHandler = self::createRequestHandler($requestsHistory, [
            new Response(200),
        ]);

        $customToken = 'custom-token';
        $client = new VariablesApiClient(
            self::BASE_URL,
            $customToken,
            requestHandler: $requestHandler(...),
        );

        $client->deleteVariable('hash');

        self::assertCount(1, $requestsHistory);
        self::assertSame($customToken, $requestsHistory[0]['request']->getHeaderLine('X-StorageApi-Token'));
    }

    public function testClientErrorMessageTrimsWhitespace(): void
    {
        $requestHandler = self::createRequestHandler($requestsHistory, [
            new Response(
                400,
                ['Content-Type' => 'application/json'],
                Json::encodeArray([
                    'error' => ' some error ',
                    'code' => ' 400 ',
                ]),
            ),
        ]);

        $client = new VariablesApiClient(
            self::BASE_URL,
            self::API_TOKEN,
            backoffMaxTries: 0,
            requestHandler: $requestHandler(...),
        );

        try {
            $client->deleteVariable('hash');
            self::fail('Expected VaultClientException to be thrown');
        } catch (VaultClientException $e) {
            self::assertSame('400 :  some error', $e->getMessage());
        }
    }

    public function testBackoffMaxTriesIsForwardedFromOptions(): void
    {
        $requestHandler = self::createRequestHandler($requestsHistory, [
            new Response(500),
            new Response(500),
            new Response(200),
        ]);

        $client = new VariablesApiClient(
            self::BASE_URL,
            self::API_TOKEN,
            backoffMaxTries: 2,
            requestHandler: $requestHandler(...),
        );

        $client->deleteVariable('hash');

        self::assertCount(3, $requestsHistory);
    }

    public function testLoggerIsForwardedFromOptions(): void
    {
        $requestHandler = self::createRequestHandler($requestsHistory, [
            new Response(200),
        ]);

        $testHandler = new TestHandler();
        $logger = new Logger('test', [$testHandler]);

        $client = new VariablesApiClient(
            self::BASE_URL,
            self::API_TOKEN,
            logger: $logger,
            requestHandler: $requestHandler(...),
        );

        $client->deleteVariable('hash');

        self::assertNotEmpty($testHandler->getRecords());
    }

    public function testDefaultBackoffMaxTriesIsUsed(): void
    {
        // Asserted via the constructor default rather than exhausting 10 real exponential-backoff
        // retries (~17 min); testBackoffMaxTriesIsForwardedFromOptions proves a value is wired
        // through to actual retries.
        $default = null;
        foreach ((new ReflectionMethod(VariablesApiClient::class, '__construct'))->getParameters() as $parameter) {
            if ($parameter->getName() === 'backoffMaxTries') {
                $default = $parameter->getDefaultValue();
            }
        }

        self::assertSame(10, $default);
    }

    public function testDefaultConnectTimeoutIsForwardedToGuzzle(): void
    {
        $requestHandler = self::createRequestHandler($requestsHistory, [
            new Response(200),
        ]);

        // Use default connectTimeout (10) — mutating it would change this assertion.
        $client = new VariablesApiClient(
            self::BASE_URL,
            self::API_TOKEN,
            requestHandler: $requestHandler(...),
        );

        $client->deleteVariable('hash');

        self::assertSame(10, $requestsHistory[0]['options']['connect_timeout']);
    }

    public function testDefaultRequestTimeoutIsForwardedToGuzzle(): void
    {
        $requestHandler = self::createRequestHandler($requestsHistory, [
            new Response(200),
        ]);

        // Use default requestTimeout (120) — mutating it would change this assertion.
        $client = new VariablesApiClient(
            self::BASE_URL,
            self::API_TOKEN,
            requestHandler: $requestHandler(...),
        );

        $client->deleteVariable('hash');

        self::assertSame(120, $requestsHistory[0]['options']['timeout']);
    }
}
