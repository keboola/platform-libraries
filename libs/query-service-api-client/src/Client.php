<?php

declare(strict_types=1);

namespace Keboola\QueryApi;

use GuzzleHttp\Client as GuzzleClient;
use GuzzleHttp\Exception\GuzzleException;
use GuzzleHttp\Exception\RequestException;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\MessageFormatter;
use GuzzleHttp\Middleware;
use InvalidArgumentException;
use JsonException;
use Keboola\QueryApi\Response\CancelJobResponse;
use Keboola\QueryApi\Response\JobResultsResponse;
use Keboola\QueryApi\Response\JobStatusResponse;
use Keboola\QueryApi\Response\SubmitQueryJobResponse;
use Keboola\QueryApi\Response\WorkspaceQueryResponse;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use Symfony\Component\Validator\Constraints\NotBlank;
use Symfony\Component\Validator\Constraints\Range;
use Symfony\Component\Validator\Constraints\Url;
use Symfony\Component\Validator\ConstraintViolationInterface;
use Symfony\Component\Validator\Validation;
use Throwable;

class Client
{
    private const string DEFAULT_USER_AGENT = 'Keboola Query API PHP Client';
    private const int DEFAULT_BACKOFF_RETRIES = 3;
    private const int GUZZLE_CONNECT_TIMEOUT_SECONDS = 10;
    private const int GUZZLE_TIMEOUT_SECONDS = 120;
    private const int DEFAULT_MAX_WAIT_SECONDS = 30;
    private GuzzleClient $client;

    /**
     * @param array{
     *     url?: string,
     *     token?: string,
     *     backoffMaxTries?: int,
     *     userAgent?: string,
     *     runId?: string,
     *     logger?: LoggerInterface,
     *     handler?: HandlerStack,
     * } $config
     * @param array<string, mixed> $options
     */
    public function __construct(array $config, array $options = [])
    {
        $validator = Validation::createValidator();
        $errors = $validator->validate($config['url'] ?? '', [new NotBlank(), new Url()]);
        $errors->addAll(
            $validator->validate($config['token'] ?? '', [new NotBlank()]),
        );

        if (!isset($options['backoffMaxTries']) || $options['backoffMaxTries'] === '') {
            $options['backoffMaxTries'] = self::DEFAULT_BACKOFF_RETRIES;
        }

        $errors->addAll($validator->validate($options['backoffMaxTries'], [new Range(['min' => 0, 'max' => 100])]));

        if ($errors->count() !== 0) {
            $messages = '';
            /** @var ConstraintViolationInterface $error */
            foreach ($errors as $error) {
                $invalidValue = $error->getInvalidValue();
                $valueStr = (is_scalar($invalidValue) ? (string) $invalidValue : '');
                $messages .= 'Value "' . $valueStr . '" is invalid: ' . $error->getMessage() . "\n";
            }
            throw new ClientException('Invalid parameters when creating client: ' . $messages);
        }

        if (!isset($options['userAgent'])) {
            $options['userAgent'] = self::DEFAULT_USER_AGENT;
        }
        if (!isset($options['logger'])) {
            $options['logger'] = new NullLogger();
        }

        /** @var array{url: string, token: string} $config */
        $this->client = $this->initClient($config['url'], $config['token'], $options);
    }

    /**
     * @param array<string, mixed> $options
     */
    private function initClient(string $url, string $token, array $options = []): GuzzleClient
    {
        /** @var array{
         *     handler?: HandlerStack,
         *     backoffMaxTries: int,
         *     logger: LoggerInterface,
         *     runId?: string,
         *     userAgent: string
         * } $options
         */

        // Initialize handlers (start with those supplied in constructor)
        $handlerStack = $options['handler'] ?? HandlerStack::create();

        // Set exponential backoff
        /** @var int<1, max> $backoffMaxTries */
        $backoffMaxTries = $options['backoffMaxTries'];
        $handlerStack->push(Middleware::retry(new RetryDecider($backoffMaxTries, $options['logger'])));
        // Set handler to set default headers
        $handlerStack->push(Middleware::mapRequest(
            function (RequestInterface $request) use ($token, $options) {
                if (isset($options['runId'])) {
                    $request = $request->withHeader('X-KBC-RunId', $options['runId']);
                }
                return $request
                    ->withHeader('User-Agent', $options['userAgent'])
                    ->withHeader('X-StorageApi-Token', $token)
                    ->withHeader('Content-type', 'application/json');
            },
        ));
        // Set client logger
        $handlerStack->push(Middleware::log(
            $options['logger'],
            new MessageFormatter(
                '{hostname} {req_header_User-Agent} - [{ts}] "{method} {resource} {protocol}/{version}"' .
                ' {code} {res_header_Content-Length}',
            ),
        ));
        // finally create the instance
        return new GuzzleClient([
            'base_uri' => $url,
            'handler' => $handlerStack,
            'connect_timeout' => self::GUZZLE_CONNECT_TIMEOUT_SECONDS,
            'timeout' => self::GUZZLE_TIMEOUT_SECONDS,
        ]);
    }

    /**
     * @param array{statements: string[], transactional?: bool} $requestBody
     */
    public function submitQueryJob(string $branchId, string $workspaceId, array $requestBody): SubmitQueryJobResponse
    {
        $url = sprintf('/api/v1/branches/%s/workspaces/%s/queries', $branchId, $workspaceId);
        $response = $this->sendRequest('POST', $url, $requestBody);
        return SubmitQueryJobResponse::fromResponse($response);
    }

    /**
     * Get job status
     */
    public function getJobStatus(string $queryJobId): JobStatusResponse
    {
        $url = sprintf('/api/v1/queries/%s', $queryJobId);
        $response = $this->sendRequest('GET', $url);
        return JobStatusResponse::fromResponse($response);
    }

    /**
     * @param array{reason?: string} $requestBody
     */
    public function cancelJob(string $queryJobId, array $requestBody = []): CancelJobResponse
    {
        $url = sprintf('/api/v1/queries/%s/cancel', $queryJobId);
        return CancelJobResponse::fromResponse($this->sendRequest('POST', $url, $requestBody));
    }

    /**
     * Get job results
     */
    public function getJobResults(string $queryJobId, string $statementId): JobResultsResponse
    {
        $url = sprintf('/api/v1/queries/%s/%s/results', $queryJobId, $statementId);
        return JobResultsResponse::fromResponse($this->sendRequest('GET', $url));
    }

    /**
     * Execute a workspace query and wait for results
     *
     * @param array{statements: string[], transactional?: bool} $requestBody
     */
    public function executeWorkspaceQuery(
        string $branchId,
        string $workspaceId,
        array $requestBody,
        int $maxWaitSeconds = self::DEFAULT_MAX_WAIT_SECONDS,
    ): WorkspaceQueryResponse {
        // Submit the query job
        $submitResponse = $this->submitQueryJob($branchId, $workspaceId, $requestBody);
        $queryJobId = $submitResponse->getQueryJobId();

        // Wait for job completion
        $finalStatus = $this->waitForJobCompletion($queryJobId, $maxWaitSeconds);

        if ($finalStatus->getStatus() !== 'completed') {
            $errorMessage = ResultHelper::extractAllStatementErrors($finalStatus->getStatements());
            throw new ClientException(
                sprintf('Query job failed with error: %s', $errorMessage),
                400,
            );
        }

        // Get results for all completed statements
        $results = [];
        foreach ($finalStatus->getStatements() as $statement) {
            if ($statement->getStatus() === 'completed') {
                $statementResponse = $this->getJobResults($queryJobId, $statement->getId());
                $results[] = $statementResponse;
            }
        }

        return new WorkspaceQueryResponse(
            $queryJobId,
            $finalStatus->getStatus(),
            $finalStatus->getStatements(),
            $results,
        );
    }


    /**
     * @param array<string, mixed>|null $requestBody
     */
    private function sendRequest(string $method, string $url, ?array $requestBody = null): ResponseInterface
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
            return $this->client->request($method, $url, $options);
        } catch (RequestException $e) {
            throw $this->processRequestException($e) ?? new ClientException($e->getMessage(), $e->getCode(), $e);
        } catch (GuzzleException $e) {
            throw new ClientException($e->getMessage(), $e->getCode(), $e);
        }
    }

    private function processRequestException(RequestException $e): ?ClientException
    {
        $response = $e->getResponse();
        if ($response === null) {
            return null;
        }

        try {
            $data = json_decode($response->getBody()->getContents(), true, 512, JSON_THROW_ON_ERROR);
        } catch (JsonException) {
            return new ClientException(trim($e->getMessage()), $response->getStatusCode(), $e);
        }

        if (!is_array($data) || empty($data['exception']) || !is_string($data['exception'])) {
            return null;
        }

        return new ClientException(
            trim($data['exception']),
            $response->getStatusCode(),
            $e,
        );
    }

    /**
     * Wait for job completion with timeout
     *
     * @param int $maxWaitSeconds Maximum time to wait in seconds
     */
    public function waitForJobCompletion(string $queryJobId, int $maxWaitSeconds = 30): JobStatusResponse
    {
        $startTime = time();

        $tries = 0;
        while (time() - $startTime < $maxWaitSeconds) {
            $statusResponse = $this->getJobStatus($queryJobId);

            if (in_array($statusResponse->getStatus(), ['completed', 'failed', 'canceled'], true)) {
                return $statusResponse;
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
