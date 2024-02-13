<?php

declare(strict_types=1);

namespace Keboola\SandboxesServiceApiClient\Tests;

use GuzzleHttp\Exception\ClientException;
use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Psr7\Response;
use Keboola\SandboxesServiceApiClient\RetryDecider;
use Monolog\Handler\TestHandler;
use Monolog\Logger;
use PHPUnit\Framework\TestCase;
use Psr\Http\Message\ResponseInterface;
use Psr\Log\LoggerInterface;
use RuntimeException;

class RetryDeciderTest extends TestCase
{
    private readonly LoggerInterface $logger;
    private readonly TestHandler $logsHandler;

    protected function setUp(): void
    {
        parent::setUp();

        $this->logsHandler = new TestHandler();
        $this->logger = new Logger('tests', [$this->logsHandler]);
    }

    /** @dataProvider provideTestData */
    public function testDecide(
        int $retries,
        ?ResponseInterface $response,
        mixed $error,
        bool $expectedResult,
        ?string $log,
    ): void {
        $decider = new RetryDecider(10, $this->logger);
        $result = $decider($retries, new Request('GET', ''), $response, $error);

        self::assertSame($expectedResult, $result);

        if ($log === null) {
            self::assertCount(0, $this->logsHandler->getRecords());
        } else {
            self::assertTrue($this->logsHandler->hasWarning($log));
        }
    }

    public function provideTestData(): iterable
    {
        yield 'too many retries' => [
            'retries' => 11,
            'response' => null,
            'error' => null,
            'result' => false,
            'log' => null,
        ];

        yield 'no error, no response' => [
            'retries' => 0,
            'response' => null,
            'error' => null,
            'result' => false,
            'log' => null,
        ];

        yield '4xx response without error' => [
            'retries' => 0,
            'response' => new Response(400),
            'error' => null,
            'result' => false,
            'log' => null,
        ];

        yield '4xx response with error' => [
            'retries' => 0,
            'response' => new Response(400),
            'error' => new ClientException('Request failed', new Request('GET', ''), new Response(400)),
            'result' => false,
            'log' => null,
        ];

        yield '5xx response' => [
            'retries' => 0,
            'response' => new Response(500, [], 'Error body'),
            'error' => null,
            'result' => true,
            'log' => 'Request failed (Error body), retrying (0 of 10)',
        ];

        yield 'throttling response' => [
            'retries' => 0,
            'response' => new Response(429, [], 'Error body'),
            'error' => null,
            'result' => true,
            'log' => 'Request failed (Error body), retrying (0 of 10)',
        ];

        yield 'text error with response' => [
            'retries' => 0,
            'response' => new Response(200),
            'error' => 'Text error',
            'result' => true,
            'log' => 'Request failed (Text error), retrying (0 of 10)',
        ];

        yield 'text error without response' => [
            'retries' => 0,
            'response' => null,
            'error' => 'Text error',
            'result' => true,
            'log' => 'Request failed (Text error), retrying (0 of 10)',
        ];

        yield 'exception error' => [
            'retries' => 0,
            'response' => new Response(200),
            'error' => new RuntimeException('Exception error'),
            'result' => true,
            'log' => 'Request failed (Exception error), retrying (0 of 10)',
        ];
    }
}
