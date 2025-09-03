<?php

declare(strict_types=1);

namespace Keboola\QueryApi;

use GuzzleHttp\Client as GuzzleClient;
use GuzzleHttp\Exception\ClientException as GuzzleClientException;
use GuzzleHttp\Exception\ConnectException;
use GuzzleHttp\Exception\GuzzleException;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Middleware;
use InvalidArgumentException;
use JsonException;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;
use Throwable;

class Client
{
    private const string DEFAULT_USER_AGENT = 'Keboola Query API PHP Client';
    private const int DEFAULT_BACKOFF_RETRIES = 3;
    private const int GUZZLE_CONNECT_TIMEOUT_SECONDS = 10;
    private const int GUZZLE_TIMEOUT_SECONDS = 120;
    private const int DEFAULT_MAX_WAIT_SECONDS = 30;

    private string $apiUrl;
    private string $tokenString;
    private int $backoffMaxTries;
    private string $userAgent;
    private GuzzleClient $client;

    /**
     * @param array{
     *     url: string,
     *     token: string,
     *     backoffMaxTries?: int,
     *     userAgent?: string,
     *     handler?: HandlerStack,
     * } $config
     */
    public function __construct(array $config)
    {
        if (empty($config['url'])) {
            throw new InvalidArgumentException('url must be set');
        }
        if (empty($config['token'])) {
            throw new InvalidArgumentException('token must be set');
        }

        $this->apiUrl = rtrim($config['url'], '/');
        $this->tokenString = $config['token'];
        $this->backoffMaxTries = $config['backoffMaxTries'] ?? self::DEFAULT_BACKOFF_RETRIES;
        $this->userAgent = self::DEFAULT_USER_AGENT;

        if (isset($config['userAgent'])) {
            $this->userAgent .= ' ' . $config['userAgent'];
        }

        $this->initClient($config);
    }

    /**
     * @param array{handler?: HandlerStack} $config
     */
    private function initClient(array $config): void
    {
        $handlerStack = $config['handler'] ?? HandlerStack::create();
        $handlerStack->push(Middleware::retry($this->createRetryDecider(), $this->createRetryDelay()));

        // Add request mapping middleware for headers
        $handlerStack->push(Middleware::mapRequest(
            function (RequestInterface $request) {
                return $this->addRequestHeaders($request);
            },
        ));

        $this->client = new GuzzleClient([
            'base_uri' => $this->apiUrl,
            'handler' => $handlerStack,
            'connect_timeout' => self::GUZZLE_CONNECT_TIMEOUT_SECONDS,
            'timeout' => self::GUZZLE_TIMEOUT_SECONDS,
        ]);
    }

    private function createRetryDecider(): callable
    {
        return function (
            int $retries,
            RequestInterface $request,
            ?ResponseInterface $response = null,
            ?Throwable $exception = null,
        ): bool {
            if ($retries >= $this->backoffMaxTries) {
                return false;
            }

            if ($exception instanceof ConnectException) {
                return true;
            }

            if ($response && $response->getStatusCode() >= 500) {
                return true;
            }

            return false;
        };
    }

    private function createRetryDelay(): callable
    {
        return function (int $numberOfRetries): int {
            return 1000 * (2 ** $numberOfRetries);
        };
    }

    /**
     * Add request headers with selective authentication
     */
    private function addRequestHeaders(RequestInterface $request): RequestInterface
    {
        $path = $request->getUri()->getPath();

        // Start with base headers that all requests need
        $baseRequest = $request
            ->withHeader('User-Agent', $this->userAgent)
            ->withHeader('Content-Type', 'application/json');

        // Skip authentication for health-check endpoints
        if (str_contains($path, '/health-check')) {
            return $baseRequest;
        }

        // Add Storage API token for all other endpoints that require authentication
        return $baseRequest->withHeader('X-StorageAPI-Token', $this->tokenString);
    }

    /**
     * Submit a new query job
     *
     * @param array{statements: string[], transactional?: bool} $requestBody
     * @return array<string, mixed>
     */
    public function submitQueryJob(string $branchId, string $workspaceId, array $requestBody): array
    {
        $url = sprintf('/api/v1/branches/%s/workspaces/%s/queries', $branchId, $workspaceId);
        return $this->sendRequest('POST', $url, $requestBody);
    }

    /**
     * Get job status
     *
     * @return array<string, mixed>
     */
    public function getJobStatus(string $queryJobId): array
    {
        $url = sprintf('/api/v1/queries/%s', $queryJobId);
        return $this->sendRequest('GET', $url);
    }

    /**
     * Cancel a job
     *
     * @param array{reason?: string} $requestBody
     * @return array<string, mixed>
     */
    public function cancelJob(string $queryJobId, array $requestBody = []): array
    {
        $url = sprintf('/api/v1/queries/%s/cancel', $queryJobId);
        return $this->sendRequest('POST', $url, $requestBody);
    }

    /**
     * Get job results
     *
     * @return array{
     *      "columns": array<int, array{
     *          "name": string,
     *          "type": "text",
     *      }>,
     *      "data": array<array<int, string>>,
     *      "status": string,
     *      "rowsAffected": int
     * }
     */
    public function getJobResults(string $queryJobId, string $statementId): array
    {
        $url = sprintf('/api/v1/queries/%s/%s/results', $queryJobId, $statementId);
        /** @phpstan-ignore-next-line */
        return $this->sendRequest('GET', $url);
    }

    /**
     * Execute a workspace query and wait for results
     *
     * @param array{statements: string[], transactional?: bool} $requestBody
     * @return array{
     *     queryJobId: string,
     *     status: string,
     *     statements: array<array<string, mixed>>,
     *     results: array{
     *          "columns": array<array{
     *              "name": string,
     *              "type": "text",
     *          }>,
     *          "data": array<array<int, string>>,
     *          "status": string,
     *          "rowsAffected": int
     *     }[],
     * }
     */
    public function executeWorkspaceQuery(
        string $branchId,
        string $workspaceId,
        array $requestBody,
        int $maxWaitSeconds = self::DEFAULT_MAX_WAIT_SECONDS,
    ): array {
        // Submit the query job
        $response = $this->submitQueryJob($branchId, $workspaceId, $requestBody);

        if (!isset($response['queryJobId']) || !is_string($response['queryJobId'])) {
            throw new ClientException('Invalid response from submitQueryJob: missing queryJobId');
        }

        $queryJobId = $response['queryJobId'];

        // Wait for job completion
        $finalStatus = $this->waitForJobCompletion($queryJobId, $maxWaitSeconds);

        if (!isset($finalStatus['status']) || $finalStatus['status'] !== 'completed') {
            throw new ClientException(
                sprintf('Query job failed with error: %s', ResultHelper::extractAllStatementErrors($finalStatus)),
                400,
            );
        }

        // Get results for all completed statements
        $results = [];
        if (isset($finalStatus['statements']) && is_array($finalStatus['statements'])) {
            foreach ($finalStatus['statements'] as $statement) {
                if (is_array($statement) && isset($statement['id']) && isset($statement['status'])) {
                    if ($statement['status'] === 'completed') {
                        $statementResults = $this->getJobResults($queryJobId, $statement['id']);
                        $results[] = $statementResults;
                    }
                }
            }
        }

        /** @var array<array<string, mixed>> $statements */
        $statements = $finalStatus['statements'] ?? [];

        return [
            'queryJobId' => $queryJobId,
            'status' => $finalStatus['status'],
            'statements' => $statements,
            'results' => $results,
        ];
    }

    /**
     * Health check
     *
     * @return array<string, mixed>
     */
    public function healthCheck(): array
    {
        return $this->sendRequest('GET', '/health-check');
    }

    /**
     * @param array<string, mixed>|null $requestBody
     * @return array<string, mixed>
     */
    private function sendRequest(string $method, string $url, ?array $requestBody = null): array
    {
        $options = [];

        if ($requestBody !== null) {
            try {
                $options['body'] = json_encode($requestBody, JSON_THROW_ON_ERROR);
            } catch (JsonException $e) {
                throw new ClientException('Failed to encode request body as JSON: ' . $e->getMessage(), 0, $e);
            }
        }

        try {
            $response = $this->client->request($method, $url, $options);
        } catch (GuzzleException $e) {
            $this->handleGuzzleException($e);
            throw new ClientException('Request failed after exception handling');
        }

        return $this->parseResponse($response);
    }

    /**
     * @return array<string, mixed>
     */
    private function parseResponse(ResponseInterface $response): array
    {
        $body = (string) $response->getBody();

        if (empty($body)) {
            return [];
        }

        try {
            $data = json_decode($body, true, 512, JSON_THROW_ON_ERROR);
        } catch (JsonException $e) {
            throw new ClientException('Response is not valid JSON: ' . $e->getMessage(), 0, $e);
        }

        if (!is_array($data)) {
            throw new ClientException('Response is not a JSON object');
        }

        return $data;
    }

    /**
     * @throws ClientException
     */
    private function handleGuzzleException(GuzzleException $e): void
    {
        if ($e instanceof GuzzleClientException && $e->hasResponse()) {
            $response = $e->getResponse();
            $statusCode = $response->getStatusCode();
            $body = (string) $response->getBody();

            try {
                $errorData = json_decode($body, true, 512, JSON_THROW_ON_ERROR);
            } catch (JsonException) {
                $errorData = null;
            }

            $message = is_array($errorData) && isset($errorData['exception']) && is_string($errorData['exception'])
                ? $errorData['exception']
                : $e->getMessage();
            $contextData = is_array($errorData) ? $errorData : null;
            throw new ClientException($message, $statusCode, $e, $contextData);
        }

        if ($e instanceof ConnectException) {
            throw new ClientException('Unable to connect to Query Service API: ' . $e->getMessage(), 0, $e);
        }

        throw new ClientException('Query Service API request failed: ' . $e->getMessage(), 0, $e);
    }

    /**
     * Wait for job completion with timeout
     *
     * @param int $maxWaitSeconds Maximum time to wait in seconds
     * @return array<string, mixed>
     */
    public function waitForJobCompletion(string $queryJobId, int $maxWaitSeconds = 30): array
    {
        $startTime = time();

        $tries = 0;
        while (time() - $startTime < $maxWaitSeconds) {
            $status = $this->getJobStatus($queryJobId);

            if (in_array($status['status'], ['completed', 'failed', 'canceled'], true)) {
                return $status;
            }

            // 60, 70, 90, 130, 210, 370, 690, 1000ms (max)
            $waitMilliseconds = min(50+pow(2, $tries)*10, 1000);
            usleep($waitMilliseconds * 1000);
            $tries++;
        }

        throw new ClientException(
            sprintf('Job %s did not complete within %d seconds', $queryJobId, $maxWaitSeconds),
        );
    }
}
