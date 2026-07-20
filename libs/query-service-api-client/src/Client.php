<?php

declare(strict_types=1);

namespace Keboola\QueryApi;

use Closure;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Middleware;
use GuzzleHttp\Psr7\Request;
use Keboola\ApiClientBase\ApiClient;
use Keboola\ApiClientBase\ApiClientOptions;
use Keboola\ApiClientBase\Auth\StorageApiTokenAuthenticator;
use Keboola\ApiClientBase\Json;
use Keboola\QueryApi\Exception\ClientException;
use Keboola\QueryApi\Response\CancelJobResponse;
use Keboola\QueryApi\Response\JobResultsResponse;
use Keboola\QueryApi\Response\JobStatusResponse;
use Keboola\QueryApi\Response\SubmitQueryJobResponse;
use Psr\Http\Message\RequestInterface;
use Psr\Log\LoggerInterface;
use Webmozart\Assert\Assert;

class Client
{
    private const string DEFAULT_USER_AGENT = 'Keboola Query API PHP Client';
    private const int DEFAULT_BACKOFF_MAX_TRIES = 3;
    private const int DEFAULT_MAX_WAIT_SECONDS = 30;
    private const int DEFAULT_MAX_POLL_WAIT_MS = 1000;

    private ApiClient $apiClient;

    /**
     * @param non-empty-string $baseUrl
     * @param non-empty-string $storageToken
     * @param int<0, max> $backoffMaxTries
     */
    public function __construct(
        string $baseUrl,
        string $storageToken,
        ?string $runId = null,
        ?LoggerInterface $logger = null,
        int $backoffMaxTries = self::DEFAULT_BACKOFF_MAX_TRIES,
        int $connectTimeout = ApiClientOptions::DEFAULT_CONNECT_TIMEOUT,
        int $requestTimeout = ApiClientOptions::DEFAULT_REQUEST_TIMEOUT,
        string $userAgent = self::DEFAULT_USER_AGENT,
        null|Closure|HandlerStack $requestHandler = null,
    ) {
        Assert::stringNotEmpty($baseUrl, 'Base URL must be a non-empty string');

        $handlerStack = $requestHandler instanceof HandlerStack
            ? $requestHandler
            : HandlerStack::create($requestHandler);
        if ($runId !== null) {
            $handlerStack->push(Middleware::mapRequest(
                static fn(RequestInterface $request): RequestInterface
                    => $request->withHeader('X-KBC-RunId', $runId),
            ));
        }

        $this->apiClient = new ApiClient(
            $baseUrl,
            new StorageApiTokenAuthenticator($storageToken),
            new ApiClientOptions(
                userAgent: $userAgent,
                backoffMaxTries: $backoffMaxTries,
                connectTimeout: $connectTimeout,
                requestTimeout: $requestTimeout,
                requestHandler: $handlerStack,
                logger: $logger,
            ),
            errorMessageResolver: new QueryApiErrorMessageResolver(),
            exceptionClass: ClientException::class,
        );
    }

    /**
     * @param array{statements: string[], transactional?: bool, refreshMetadataOnSuccess?: bool} $requestBody
     */
    public function submitQueryJob(string $branchId, string $workspaceId, array $requestBody): SubmitQueryJobResponse
    {
        return $this->apiClient->sendRequestAndMapResponse(
            new Request(
                'POST',
                sprintf('api/v1/branches/%s/workspaces/%s/queries', $branchId, $workspaceId),
                $this->requestHeaders(withJsonBody: true),
                Json::encodeArray($requestBody),
            ),
            SubmitQueryJobResponse::class,
        );
    }

    public function getJobStatus(string $queryJobId): JobStatusResponse
    {
        return $this->apiClient->sendRequestAndMapResponse(
            new Request('GET', sprintf('api/v1/queries/%s', $queryJobId), $this->requestHeaders()),
            JobStatusResponse::class,
        );
    }

    /**
     * @param array{reason?: string} $requestBody
     */
    public function cancelJob(string $queryJobId, array $requestBody = []): CancelJobResponse
    {
        return $this->apiClient->sendRequestAndMapResponse(
            new Request(
                'POST',
                sprintf('api/v1/queries/%s/cancel', $queryJobId),
                $this->requestHeaders(withJsonBody: true),
                Json::encodeArray($requestBody),
            ),
            CancelJobResponse::class,
        );
    }

    public function getJobResults(
        string $queryJobId,
        string $statementId,
        ?int $pageSize = null,
        ?int $offset = null,
    ): JobResultsResponse {
        $url = sprintf('api/v1/queries/%s/%s/results', $queryJobId, $statementId);

        $queryParams = [];
        if ($pageSize !== null) {
            $queryParams['pageSize'] = $pageSize;
        }
        if ($offset !== null) {
            $queryParams['offset'] = $offset;
        }
        if ($queryParams !== []) {
            $url .= '?' . http_build_query($queryParams);
        }

        return $this->apiClient->sendRequestAndMapResponse(
            new Request('GET', $url, $this->requestHeaders()),
            JobResultsResponse::class,
        );
    }

    public function waitForJobCompletion(
        string $queryJobId,
        int $maxWaitSeconds = self::DEFAULT_MAX_WAIT_SECONDS,
        int $maxPollWaitMs = self::DEFAULT_MAX_POLL_WAIT_MS,
    ): JobStatusResponse {
        $startTime = time();

        $tries = 0;
        while (time() - $startTime < $maxWaitSeconds) {
            $statusResponse = $this->getJobStatus($queryJobId);

            if (in_array($statusResponse->getStatus(), ['completed', 'failed', 'canceled'], true)) {
                return $statusResponse;
            }

            // 60, 70, 90, 130, 210, 370, 690, 1000ms (default max)
            $waitMilliseconds = min(50 + pow(2, $tries) * 10, $maxPollWaitMs);
            usleep($waitMilliseconds * 1000);
            $tries++;
        }

        throw new ClientException(
            sprintf('Job %s did not complete within %d seconds', $queryJobId, $maxWaitSeconds),
        );
    }

    /**
     * @return array<string, string>
     */
    private function requestHeaders(bool $withJsonBody = false): array
    {
        $headers = [];
        if ($withJsonBody) {
            $headers['Content-Type'] = 'application/json';
        }

        return $headers;
    }
}
