<?php

declare(strict_types=1);

namespace Keboola\SyncActionsClient\Tests;

use GuzzleHttp\Psr7\Response;
use InvalidArgumentException;
use Keboola\SyncActionsClient\ActionData;
use Keboola\SyncActionsClient\Exception\SyncActionsClientException;
use Keboola\SyncActionsClient\SyncActionsApiClient;
use Monolog\Handler\TestHandler;
use Monolog\Logger;
use PHPUnit\Framework\TestCase;
use ReflectionMethod;
use stdClass;

class SyncActionsApiClientTest extends TestCase
{
    use ApiClientTestTrait;

    private const BASE_URL = 'http://example.com/';
    private const TOKEN = 'testToken';
    private const COMPONENT_ID = 'keboola.runner-config-test';

    public function testEmptyTokenThrows(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Storage API token must not be empty');

        new SyncActionsApiClient(self::BASE_URL, ''); // @phpstan-ignore-line
    }

    public function testEmptyBaseUrlThrows(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Base URL must be a non-empty string');

        new SyncActionsApiClient('', self::TOKEN); // @phpstan-ignore-line
    }

    public function testCallActionSendsRequestAndMapsResponse(): void
    {
        $requestHandler = self::createRequestHandler($requestsHistory, [
            new Response(201, ['Content-Type' => 'application/json'], '{"foo": "bar"}'),
        ]);

        $client = new SyncActionsApiClient(self::BASE_URL, self::TOKEN, requestHandler: $requestHandler(...));

        $response = $client->callAction(new ActionData(self::COMPONENT_ID, 'someAction'));

        self::assertSame('bar', $response->data->foo);
        self::assertCount(1, $requestsHistory);
        self::assertRequestEquals(
            'POST',
            'http://example.com/actions',
            [
                'X-StorageApi-Token' => self::TOKEN,
                'User-Agent' => 'Sync Actions PHP Client',
                'Content-Type' => 'application/json',
            ],
            '{"componentId":"keboola.runner-config-test","action":"someAction","configData":[]}',
            $requestsHistory[0]['request'],
        );
    }

    public function testCallActionPreservesObjectFidelity(): void
    {
        // Regression guard: decoding via an associative array collapses {} to [] and renumbers
        // integer-keyed objects, so callAction must decode the raw body straight to stdClass.
        $requestHandler = self::createRequestHandler($requestsHistory, [
            new Response(200, [], '{"filled": {"x": "y"}, "empty": {}, "nested": {"inner": {}}}'),
        ]);

        $client = new SyncActionsApiClient(self::BASE_URL, self::TOKEN, requestHandler: $requestHandler(...));

        $response = $client->callAction(new ActionData(self::COMPONENT_ID, 'someAction'));

        self::assertInstanceOf(stdClass::class, $response->data->filled);
        self::assertSame('y', $response->data->filled->x);
        self::assertInstanceOf(stdClass::class, $response->data->empty);
        self::assertInstanceOf(stdClass::class, $response->data->nested);
        self::assertInstanceOf(stdClass::class, $response->data->nested->inner);
    }

    public function testCallActionWithUnencodableDataThrows(): void
    {
        $requestHandler = self::createRequestHandler($requestsHistory, [new Response(200)]);
        $client = new SyncActionsApiClient(self::BASE_URL, self::TOKEN, requestHandler: $requestHandler(...));

        $resource = fopen(sys_get_temp_dir() . '/touch', 'w');

        $this->expectException(SyncActionsClientException::class);
        $this->expectExceptionMessage('Invalid job data: Type is not supported');

        $client->callAction(new ActionData(self::COMPONENT_ID, 'someAction', ['res' => $resource]));
    }

    public function testServerErrorIsWrappedAsClientException(): void
    {
        $requestHandler = self::createRequestHandler($requestsHistory, [
            new Response(500, ['Content-Type' => 'text/plain'], 'Error on server'),
        ]);

        $client = new SyncActionsApiClient(
            self::BASE_URL,
            self::TOKEN,
            backoffMaxTries: 0,
            requestHandler: $requestHandler(...),
        );

        $this->expectException(SyncActionsClientException::class);
        $this->expectExceptionMessage('Error on server');

        $client->callAction(new ActionData(self::COMPONENT_ID, 'someAction'));
    }

    public function testInvalidJsonResponseIsWrappedAsClientException(): void
    {
        $requestHandler = self::createRequestHandler($requestsHistory, [
            new Response(200, ['Content-Type' => 'application/json'], '{not a valid json]'),
        ]);

        $client = new SyncActionsApiClient(self::BASE_URL, self::TOKEN, requestHandler: $requestHandler(...));

        $this->expectException(SyncActionsClientException::class);
        $this->expectExceptionMessage('Response is not valid JSON: Syntax error');

        $client->callAction(new ActionData(self::COMPONENT_ID, 'someAction'));
    }

    public function testErrorResponseWithErrorAndCodeIsFormatted(): void
    {
        $requestHandler = self::createRequestHandler($requestsHistory, [
            new Response(400, ['Content-Type' => 'application/json'], '{"error": "Missing data", "code": 400}'),
        ]);

        $client = new SyncActionsApiClient(
            self::BASE_URL,
            self::TOKEN,
            backoffMaxTries: 0,
            requestHandler: $requestHandler(...),
        );

        $this->expectException(SyncActionsClientException::class);
        $this->expectExceptionMessage('400: Missing data');

        $client->callAction(new ActionData(self::COMPONENT_ID, 'someAction'));
    }

    public function testErrorWithoutErrorEnvelopeFallsBackToGuzzleMessage(): void
    {
        $requestHandler = self::createRequestHandler($requestsHistory, [
            new Response(400, ['Content-Type' => 'application/json'], '{"context": {"errorCode": "some.error"}}'),
        ]);

        $client = new SyncActionsApiClient(
            self::BASE_URL,
            self::TOKEN,
            backoffMaxTries: 0,
            requestHandler: $requestHandler(...),
        );

        $this->expectException(SyncActionsClientException::class);
        $this->expectExceptionMessage('400 Bad Request');

        $client->callAction(new ActionData(self::COMPONENT_ID, 'someAction'));
    }

    public function testUserAgentSuffixIsAppliedAndRequestIsLogged(): void
    {
        $requestHandler = self::createRequestHandler($requestsHistory, [
            new Response(200, ['Content-Type' => 'application/json'], '{"foo": "bar"}'),
        ]);

        $logHandler = new TestHandler();
        $logger = new Logger('test', [$logHandler]);

        $client = new SyncActionsApiClient(
            self::BASE_URL,
            self::TOKEN,
            logger: $logger,
            userAgent: 'test agent',
            requestHandler: $requestHandler(...),
        );

        $client->callAction(new ActionData(self::COMPONENT_ID, 'someAction'));

        self::assertSame(
            'Sync Actions PHP Client - test agent',
            $requestsHistory[0]['request']->getHeaderLine('User-Agent'),
        );
        self::assertNotEmpty($logHandler->getRecords());
    }

    public function testRetriesOnServerErrorByDefault(): void
    {
        $requestHandler = self::createRequestHandler($requestsHistory, [
            new Response(500, [], 'fail'),
            new Response(200, ['Content-Type' => 'application/json'], '{"foo": "bar"}'),
        ]);

        $client = new SyncActionsApiClient(self::BASE_URL, self::TOKEN, requestHandler: $requestHandler(...));

        $response = $client->callAction(new ActionData(self::COMPONENT_ID, 'someAction'));

        self::assertSame('bar', $response->data->foo);
        self::assertCount(2, $requestsHistory);
    }

    public function testStopsRetryingAfterMaxTries(): void
    {
        $requestHandler = self::createRequestHandler($requestsHistory, [
            new Response(500, [], 'fail'),
            new Response(500, [], 'fail'),
        ]);

        $client = new SyncActionsApiClient(
            self::BASE_URL,
            self::TOKEN,
            backoffMaxTries: 1,
            requestHandler: $requestHandler(...),
        );

        try {
            $client->callAction(new ActionData(self::COMPONENT_ID, 'someAction'));
            self::fail('Expected SyncActionsClientException to be thrown');
        } catch (SyncActionsClientException $e) {
            self::assertStringContainsString('500 Internal Server Error', $e->getMessage());
        }

        self::assertCount(2, $requestsHistory);
    }

    public function testDoesNotRetryOnClientError(): void
    {
        $requestHandler = self::createRequestHandler($requestsHistory, [
            new Response(401, ['Content-Type' => 'application/json'], '{"message": "Unauthorized"}'),
        ]);

        $client = new SyncActionsApiClient(self::BASE_URL, self::TOKEN, requestHandler: $requestHandler(...));

        try {
            $client->callAction(new ActionData(self::COMPONENT_ID, 'someAction'));
            self::fail('Expected SyncActionsClientException to be thrown');
        } catch (SyncActionsClientException $e) {
            self::assertStringContainsString('Unauthorized', $e->getMessage());
        }

        self::assertCount(1, $requestsHistory);
    }

    public function testGetActions(): void
    {
        $requestHandler = self::createRequestHandler($requestsHistory, [
            new Response(200, ['Content-Type' => 'application/json'], '{"actions": ["action1", "action2"]}'),
        ]);

        $client = new SyncActionsApiClient(self::BASE_URL, self::TOKEN, requestHandler: $requestHandler(...));

        $response = $client->getActions(self::COMPONENT_ID);

        self::assertSame(['action1', 'action2'], $response->actions);
        self::assertRequestEquals(
            'GET',
            'http://example.com/actions?componentId=keboola.runner-config-test',
            ['X-StorageApi-Token' => self::TOKEN],
            null,
            $requestsHistory[0]['request'],
        );
    }

    public function testGetActionsWithInvalidResponseThrows(): void
    {
        $requestHandler = self::createRequestHandler($requestsHistory, [
            new Response(200, ['Content-Type' => 'application/json'], '{"broken": ["action1"]}'),
        ]);

        $client = new SyncActionsApiClient(self::BASE_URL, self::TOKEN, requestHandler: $requestHandler(...));

        $this->expectException(SyncActionsClientException::class);
        $this->expectExceptionMessage('Failed to map response data');

        $client->getActions(self::COMPONENT_ID);
    }

    public function testDefaultBackoffMaxTriesIsTen(): void
    {
        $default = null;
        foreach ((new ReflectionMethod(SyncActionsApiClient::class, '__construct'))->getParameters() as $parameter) {
            if ($parameter->getName() === 'backoffMaxTries') {
                $default = $parameter->getDefaultValue();
            }
        }

        self::assertSame(10, $default);
    }
}
