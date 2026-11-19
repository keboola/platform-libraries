<?php

declare(strict_types=1);

namespace Keboola\AppsApiClient\Tests;

use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Psr7\Response;
use Keboola\AppsApiClient\AppsApiClient;
use Keboola\AppsApiClient\Exception\AppsClientException;
use Keboola\AppsApiClient\Model\App;
use Keboola\AppsApiClient\Model\Deployment;
use PHPUnit\Framework\TestCase;
use Psr\Http\Message\RequestInterface;

class AppsApiClientTest extends TestCase
{
    /**
     * @param int<0, max> $backoffMaxTries
     */
    private function buildClient(MockHandler $mock, int $backoffMaxTries = 0): AppsApiClient
    {
        return new AppsApiClient(
            'https://apps.keboola.com',
            'my-token',
            backoffMaxTries: $backoffMaxTries,
            requestHandler: HandlerStack::create($mock),
        );
    }

    private static function lastRequest(MockHandler $mock): RequestInterface
    {
        $request = $mock->getLastRequest();
        self::assertNotNull($request);
        self::assertSame('my-token', $request->getHeaderLine('X-StorageApi-Token'));
        return $request;
    }

    public function testGetApp(): void
    {
        $mock = new MockHandler([new Response(200, [], (string) json_encode(AppTestData::app()))]);

        $app = $this->buildClient($mock)->getApp('123');

        self::assertSame('123', $app->id);
        self::assertSame(App::STATE_RUNNING, $app->state);
        $request = self::lastRequest($mock);
        self::assertSame('GET', $request->getMethod());
        self::assertSame('https://apps.keboola.com/apps/123', (string) $request->getUri());
    }

    public function testResumeApp(): void
    {
        $mock = new MockHandler([
            new Response(202, [], (string) json_encode(AppTestData::app(['state' => 'starting']))),
        ]);

        $app = $this->buildClient($mock)->resumeApp('123');

        self::assertSame(App::STATE_STARTING, $app->state);
        $request = self::lastRequest($mock);
        self::assertSame('POST', $request->getMethod());
        self::assertSame('https://apps.keboola.com/apps/123/resume', (string) $request->getUri());
    }

    public function testCreateDeploymentOfLatestConfigVersion(): void
    {
        $mock = new MockHandler([new Response(202, [], (string) json_encode(AppTestData::deployment()))]);

        $deployment = $this->buildClient($mock)->createDeployment('123');

        self::assertSame(AppTestData::DEPLOYMENT_ID, $deployment->id);
        $request = self::lastRequest($mock);
        self::assertSame('POST', $request->getMethod());
        self::assertSame('https://apps.keboola.com/apps/123/deployments', (string) $request->getUri());
        self::assertSame('application/json', $request->getHeaderLine('Content-Type'));
        self::assertSame('{}', (string) $request->getBody());
    }

    public function testCreateDeploymentWithOptions(): void
    {
        $mock = new MockHandler([new Response(202, [], (string) json_encode(AppTestData::deployment()))]);

        $this->buildClient($mock)->createDeployment('123', configVersion: '7', promote: false);

        self::assertSame(
            '{"configVersion":"7","promote":false}',
            (string) self::lastRequest($mock)->getBody(),
        );
    }

    public function testGetAppIsRetriedOn5xx(): void
    {
        $mock = new MockHandler([
            new Response(500),
            new Response(200, [], (string) json_encode(AppTestData::app())),
        ]);

        $app = $this->buildClient($mock, backoffMaxTries: 1)->getApp('123');

        self::assertSame('123', $app->id);
        self::assertCount(0, $mock);
    }

    public function testGetDeployment(): void
    {
        $mock = new MockHandler([
            new Response(200, [], (string) json_encode(AppTestData::deployment(['runState' => 'failed']))),
        ]);

        $deployment = $this->buildClient($mock)->getDeployment('123', AppTestData::DEPLOYMENT_ID);

        self::assertSame(Deployment::RUN_STATE_FAILED, $deployment->runState);
        $request = self::lastRequest($mock);
        self::assertSame('GET', $request->getMethod());
        self::assertSame(
            'https://apps.keboola.com/apps/123/deployments/' . AppTestData::DEPLOYMENT_ID,
            (string) $request->getUri(),
        );
    }

    public function testPathSegmentsAreEncoded(): void
    {
        $mock = new MockHandler([new Response(200, [], (string) json_encode(AppTestData::app()))]);

        $this->buildClient($mock)->getApp('a/b');

        self::assertSame('https://apps.keboola.com/apps/a%2Fb', (string) self::lastRequest($mock)->getUri());
    }

    public function testErrorResponseThrowsExceptionWithApiMessage(): void
    {
        $mock = new MockHandler([
            new Response(409, [], (string) json_encode(['error' => 'App "123" has no currently serving deployment'])),
        ]);

        try {
            $this->buildClient($mock)->resumeApp('123');
            self::fail('Expected exception was not thrown.');
        } catch (AppsClientException $e) {
            self::assertSame('App "123" has no currently serving deployment', $e->getMessage());
            self::assertSame(409, $e->getStatusCode());
        }
    }

    public function testErrorResponseWithoutJsonThrowsException(): void
    {
        $mock = new MockHandler([new Response(502, [], '<html>Bad Gateway</html>')]);

        try {
            $this->buildClient($mock)->getApp('123');
            self::fail('Expected exception was not thrown.');
        } catch (AppsClientException $e) {
            self::assertSame(502, $e->getStatusCode());
            self::assertStringContainsString('502 Bad Gateway', $e->getMessage());
        }
    }

    public function testInvalidJsonResponseThrowsException(): void
    {
        $mock = new MockHandler([new Response(200, [], 'not json')]);

        $this->expectException(AppsClientException::class);
        $this->expectExceptionMessage('Response is not valid JSON');

        $this->buildClient($mock)->getApp('123');
    }

    public function testUnexpectedResponseShapeThrowsException(): void
    {
        $mock = new MockHandler([new Response(200, [], (string) json_encode(['id' => '123']))]);

        $this->expectException(AppsClientException::class);
        $this->expectExceptionMessage('Failed to map response data');

        $this->buildClient($mock)->getApp('123');
    }
}
