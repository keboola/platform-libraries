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
use Keboola\QueryApi\Response\ExecuteQueryResponse;
use Keboola\QueryApi\Response\HealthCheckResponse;
use Keboola\QueryApi\Response\JobResultsResponse;
use Keboola\QueryApi\Response\JobStatusResponse;
use Keboola\QueryApi\Response\QueryJobResponse;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

class Client
{
    private const DEFAULT_USER_AGENT = 'Keboola Query API PHP Client';
    private const DEFAULT_BACKOFF_RETRIES = 3;
    private const GUZZLE_CONNECT_TIMEOUT_SECONDS = 10;
    private const GUZZLE_TIMEOUT_SECONDS = 120;
    private const DEFAULT_MAX_WAIT_SECONDS = 30;

    private string $apiUrl;
    private string $tokenString;
    private int $backoffMaxTries;
    private string $userAgent;
    private ?string $runId;
    private GuzzleClient $client;
    private LoggerInterface $logger;

    /**
     * @param array{
     *     url: string,
     *     token: string,
     *     backoffMaxTries?: int,
     *     userAgent?: string,
     *     runId?: string,
     *     handler?: HandlerStack,
     *     logger?: LoggerInterface,
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
        $this->runId = isset($config['runId']) ? (string) $config['runId'] : null;
        $this->logger = $config['logger'] ?? new NullLogger();

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

        if ($this->backoffMaxTries < 1) {
            throw new InvalidArgumentException('backoffMaxTries must be at least 1');
        }

        $retryDecider = new RetryDecider($this->backoffMaxTries, $this->logger);
        $handlerStack->push(Middleware::retry($retryDecider));

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

    private function addRequestHeaders(RequestInterface $request): RequestInterface
    {
        $baseRequest = $request
            ->withHeader('User-Agent', $this->userAgent)
            ->withHeader('Content-Type', 'application/json')
            ->withHeader('X-StorageAPI-Token', $this->tokenString);

        if ($this->runId !== null) {
            $baseRequest = $baseRequest->withHeader('X-KBC-RunId', $this->runId);
        }

        return $baseRequest;
    }

    /**
     * @param array{statements: string[], transactional?: bool} $requestBody
     */
    public function submitQueryJob(string $branchId, string $workspaceId, array $requestBody): QueryJobResponse
    {
        $url = sprintf('/api/v1/branches/%s/workspaces/%s/queries', $branchId, $workspaceId);
        $data = $this->sendRequest('POST', $url, $requestBody);
        return QueryJobResponse::fromArray($data);
    }

    public function getJobStatus(string $queryJobId): JobStatusResponse
    {
        $url = sprintf('/api/v1/queries/%s', $queryJobId);
        $data = $this->sendRequest('GET', $url);
        return JobStatusResponse::fromArray($data);
    }

    /**
     * @param array{reason?: string} $requestBody
     */
    public function cancelJob(string $queryJobId, array $requestBody = []): JobStatusResponse
    {
        $url = sprintf('/api/v1/queries/%s/cancel', $queryJobId);
        $data = $this->sendRequest('POST', $url, $requestBody);
        return JobStatusResponse::fromArray($data);
    }

    public function getJobResults(string $queryJobId, string $statementId): JobResultsResponse
    {
        $url = sprintf('/api/v1/queries/%s/%s/results', $queryJobId, $statementId);
        $data = $this->sendRequest('GET', $url);
        return JobResultsResponse::fromArray($data);
    }

    /**
     * @param array{statements: string[], transactional?: bool} $requestBody
     */
    public function executeWorkspaceQuery(
        string $branchId,
        string $workspaceId,
        array $requestBody,
        int $maxWaitSeconds = self::DEFAULT_MAX_WAIT_SECONDS,
    ): ExecuteQueryResponse {
        $response = $this->submitQueryJob($branchId, $workspaceId, $requestBody);
        $queryJobId = $response->getQueryJobId();

        $finalStatus = $this->waitForJobCompletion($queryJobId, $maxWaitSeconds);

        if (!$finalStatus->isCompleted()) {
            $errorMessage = ResultHelper::extractAllStatementErrors($finalStatus->getRawData());
            throw new ClientException(
                sprintf('Query job failed with error: %s', $errorMessage),
                400,
            );
        }

        $results = [];
        foreach ($finalStatus->getStatements() as $statement) {
            if (is_array($statement) && isset($statement['id']) && isset($statement['status'])) {
                if ($statement['status'] === 'completed' && is_string($statement['id'])) {
                    $results[] = $this->getJobResults($queryJobId, $statement['id']);
                }
            }
        }

        return new ExecuteQueryResponse(
            $queryJobId,
            $finalStatus->getStatus(),
            $finalStatus->getStatements(),
            $results,
        );
    }

    public function healthCheck(): HealthCheckResponse
    {
        $data = $this->sendRequest('GET', '/health-check');
        return HealthCheckResponse::fromArray($data);
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
    private function handleGuzzleException(GuzzleException $e): never
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

    public function waitForJobCompletion(string $queryJobId, int $maxWaitSeconds = 30): JobStatusResponse
    {
        $startTime = time();

        $tries = 0;
        while (time() - $startTime < $maxWaitSeconds) {
            $status = $this->getJobStatus($queryJobId);

            if ($status->isCompleted() || $status->isFailed() || $status->isCanceled()) {
                return $status;
            }

            $waitMilliseconds = min(50+pow(2, $tries)*10, 1000);
            usleep($waitMilliseconds * 1000);
            $tries++;
        }

        throw new ClientException(
            sprintf('Job %s did not complete within %d seconds', $queryJobId, $maxWaitSeconds),
        );
    }
}
