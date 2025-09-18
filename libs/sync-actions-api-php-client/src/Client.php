<?php

declare(strict_types=1);

namespace Keboola\SyncActionsClient;

use GuzzleHttp\Client as GuzzleClient;
use GuzzleHttp\Exception\GuzzleException;
use GuzzleHttp\Exception\RequestException;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\MessageFormatter;
use GuzzleHttp\Middleware;
use GuzzleHttp\Psr7\Request;
use JsonException;
use Keboola\SyncActionsClient\Exception\ClientException;
use Keboola\SyncActionsClient\Exception\ClientException as SyncActionsClientException;
use Keboola\SyncActionsClient\Model\ActionResponse;
use Keboola\SyncActionsClient\Model\ListActionsResponse;
use Keboola\SyncActionsClient\Model\ResponseModelInterface;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;
use Symfony\Component\Validator\Constraints\NotBlank;
use Symfony\Component\Validator\Constraints\Range;
use Symfony\Component\Validator\Constraints\Url;
use Symfony\Component\Validator\ConstraintViolationInterface;
use Symfony\Component\Validator\Validation;
use Throwable;

class Client
{
    private const string DEFAULT_USER_AGENT = 'Sync Actions PHP Client';

    private readonly HandlerStack $requestHandlerStack;
    private readonly GuzzleClient $httpClient;

    public function __construct(
        string $baseUrl,
        string $token,
        ?ApiClientConfiguration $configuration = null,
    ) {
        $configuration ??= new ApiClientConfiguration();
        $validator = Validation::createValidator();
        $errors = $validator->validate($baseUrl, [new Url()]);
        $errors->addAll(
            $validator->validate($token, [new NotBlank()]),
        );

        $errors->addAll($validator->validate(
            $configuration->backoffMaxTries,
            [new Range(['min' => 0, 'max' => 100])],
        ));

        if ($errors->count() !== 0) {
            $messages = '';
            /** @var ConstraintViolationInterface $error */
            foreach ($errors as $error) {
                assert(is_scalar($error->getInvalidValue()));
                $messages .= 'Value "' . $error->getInvalidValue() . '" is invalid: ' . $error->getMessage() . "\n";
            }
            throw new SyncActionsClientException('Invalid parameters when creating client: ' . $messages);
        }

        $this->requestHandlerStack = HandlerStack::create($configuration->requestHandler);
        $this->requestHandlerStack->push(Middleware::mapRequest(new StorageApiTokenAuthenticator($token)));

        if ($configuration->backoffMaxTries > 0) {
            $this->requestHandlerStack->push(Middleware::retry(new RetryDecider(
                $configuration->backoffMaxTries,
                $configuration->logger,
            )));
        }

        $this->requestHandlerStack->push(Middleware::log($configuration->logger, new MessageFormatter(
            '{hostname} {req_header_User-Agent} - [{ts}] "{method} {resource} {protocol}/{version}"' .
            ' {code} {res_header_Content-Length}',
        )));

        $userAgent = self::DEFAULT_USER_AGENT;
        if ($configuration->userAgent) {
            $userAgent .= ' - ' . $configuration->userAgent;
        }

        $this->httpClient = new GuzzleClient([
            'base_uri' => $baseUrl,
            'handler' => $this->requestHandlerStack,
            'headers' => [
                'User-Agent' => $userAgent,
                'Content-Type' => 'application/json',
            ],
            'connect_timeout' => 10,
            'timeout' => 120,
        ]);
    }

    /**
     * @template TResponseClass of ResponseModelInterface
     * @param class-string<TResponseClass> $responseClass
     * @return TResponseClass
     */
    public function sendRequestAndMapResponse(
        RequestInterface $request,
        string $responseClass,
        array $options = [],
    ) {
        $response = $this->doSendRequest($request, $options);
        $responseData = $response->getBody()->getContents();
        try {
            $data = (object) json_decode(
                $responseData,
                false,
                flags: JSON_THROW_ON_ERROR,
            );
        } catch (JsonException $e) {
            throw new ClientException(
                'Response is not a valid JSON: ' . $e->getMessage() . ' ' . $responseData,
                $e->getCode(),
                $e,
            );
        }

        try {
            return $responseClass::fromResponseData($data);
        } catch (Throwable $e) {
            throw new ClientException('Failed to parse response: ' . $e->getMessage(), $e->getCode(), $e);
        }
    }

    private function doSendRequest(RequestInterface $request, array $options = []): ResponseInterface
    {
        try {
            return $this->httpClient->send($request, $options);
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
            $responseData = $response->getBody()->getContents();
            $data = json_decode($responseData, false, flags: JSON_THROW_ON_ERROR);
            if (!is_object($data)) {
                throw new ClientException(
                    'Response is not a valid error response: ' . $responseData,
                );
            }
        } catch (JsonException) {
            // throw the original one, we don't care about e2
            return new ClientException(trim($e->getMessage()), $response->getStatusCode(), $e);
        }

        if (empty($data->error) || empty($data->code) || !is_scalar($data->error) || !is_scalar($data->code)) {
            return null;
        }

        return new ClientException(
            trim($data->code . ': ' . $data->error),
            $response->getStatusCode(),
            $e,
        );
    }

    public function callAction(ActionData $actionData): ActionResponse
    {
        try {
            $jobDataJson = json_encode($actionData->getArray(), JSON_THROW_ON_ERROR);
            $request = new Request('POST', 'actions', [], $jobDataJson);
        } catch (JsonException $e) {
            throw new ClientException('Invalid job data: ' . $e->getMessage(), $e->getCode(), $e);
        }
        return $this->sendRequestAndMapResponse($request, ActionResponse::class);
    }

    public function getActions(string $componentId): ListActionsResponse
    {
        $request = new Request('GET', sprintf('actions?componentId=%s', $componentId));
        return $this->sendRequestAndMapResponse($request, ListActionsResponse::class);
    }
}
