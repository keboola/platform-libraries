<?php

declare(strict_types=1);

namespace Keboola\SyncActionsClient;

use Closure;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Psr7\Request;
use JsonException;
use Keboola\ApiClientBase\ApiClient;
use Keboola\ApiClientBase\ApiClientOptions;
use Keboola\ApiClientBase\Auth\StorageApiTokenAuthenticator;
use Keboola\ApiClientBase\Json;
use Keboola\SyncActionsClient\Exception\SyncActionsClientException;
use Keboola\SyncActionsClient\Model\ActionResponse;
use Keboola\SyncActionsClient\Model\ListActionsResponse;
use Psr\Log\LoggerInterface;
use stdClass;
use Webmozart\Assert\Assert;

class SyncActionsApiClient
{
    private const FALLBACK_USER_AGENT = 'Sync Actions PHP Client';
    private const DEFAULT_BACKOFF_MAX_TRIES = 10;

    private readonly ApiClient $apiClient;

    /**
     * @param non-empty-string $baseUrl
     * @param non-empty-string $token
     * @param int<0, max> $backoffMaxTries
     */
    public function __construct(
        string $baseUrl,
        string $token,
        ?LoggerInterface $logger = null,
        int $backoffMaxTries = self::DEFAULT_BACKOFF_MAX_TRIES,
        int $connectTimeout = ApiClientOptions::DEFAULT_CONNECT_TIMEOUT,
        int $requestTimeout = ApiClientOptions::DEFAULT_REQUEST_TIMEOUT,
        ?string $userAgent = null,
        null|Closure|HandlerStack $requestHandler = null,
    ) {
        Assert::stringNotEmpty($baseUrl, 'Base URL must be a non-empty string');

        $fullUserAgent = self::FALLBACK_USER_AGENT;
        if ($userAgent !== null && $userAgent !== '') {
            $fullUserAgent .= ' - ' . $userAgent;
        }

        $this->apiClient = new ApiClient(
            $baseUrl,
            new StorageApiTokenAuthenticator($token),
            new ApiClientOptions(
                userAgent: $fullUserAgent,
                backoffMaxTries: $backoffMaxTries,
                connectTimeout: $connectTimeout,
                requestTimeout: $requestTimeout,
                requestHandler: $requestHandler,
                logger: $logger,
            ),
            errorMessageResolver: new SyncActionsErrorMessageResolver(),
            exceptionClass: SyncActionsClientException::class,
        );
    }

    public function callAction(ActionData $actionData): ActionResponse
    {
        try {
            $body = Json::encodeArray($actionData->getArray());
        } catch (JsonException $e) {
            throw new SyncActionsClientException('Invalid job data: ' . $e->getMessage(), $e->getCode(), $e);
        }

        $response = $this->apiClient->sendRequest(
            new Request('POST', 'actions', ['Content-Type' => 'application/json'], $body),
        );

        // Decode straight to stdClass: a sync action returns an arbitrary, component-defined
        // payload, and the base client's array decode would collapse empty/integer-keyed
        // objects ({} -> [], {"0":..} -> [..]). Callers navigate the result as $response->data->x.
        $responseBody = (string) $response->getBody();
        try {
            $decoded = json_decode($responseBody, false, 512, JSON_THROW_ON_ERROR);
        } catch (JsonException $e) {
            throw new SyncActionsClientException(
                'Response is not valid JSON: ' . $e->getMessage(),
                0,
                $e,
                $response->getStatusCode(),
                $responseBody,
            );
        }

        /** @var stdClass $data */
        $data = (object) $decoded;

        return new ActionResponse($data);
    }

    public function getActions(string $componentId): ListActionsResponse
    {
        return $this->apiClient->sendRequestAndMapResponse(
            new Request('GET', sprintf('actions?componentId=%s', $componentId)),
            ListActionsResponse::class,
        );
    }
}
