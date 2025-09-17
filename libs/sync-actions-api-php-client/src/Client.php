<?php

declare(strict_types=1);

namespace Keboola\SyncActionsClient;

use Closure;
use GuzzleHttp\Client as GuzzleClient;
use GuzzleHttp\Exception\ClientException;
use GuzzleHttp\Exception\GuzzleException;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\MessageFormatter;
use GuzzleHttp\Middleware;
use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Psr7\Uri;
use JsonException;
use Keboola\SyncActionsClient\Exception\ClientException as SyncActionsClientException;
use Keboola\SyncActionsClient\Exception\ResponseException;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;
use Psr\Log\LoggerInterface;
use Symfony\Component\Validator\Constraints\NotBlank;
use Symfony\Component\Validator\Constraints\Range;
use Symfony\Component\Validator\Constraints\Url;
use Symfony\Component\Validator\ConstraintViolationInterface;
use Symfony\Component\Validator\Validation;

class Client
{
    private const DEFAULT_USER_AGENT = 'Sync Actions PHP Client';
    private const DEFAULT_BACKOFF_RETRIES = 3;
    private const JSON_DEPTH = 512;

    protected GuzzleClient $guzzle;

    public function __construct(string $publicApiUrl, string $storageToken, array $options = [])
    {
        $validator = Validation::createValidator();
        $errors = $validator->validate($publicApiUrl, [new Url()]);
        $errors->addAll(
            $validator->validate($storageToken, [new NotBlank()]),
        );

        if (!isset($options['backoffMaxTries']) || $options['backoffMaxTries'] === '') {
            $options['backoffMaxTries'] = self::DEFAULT_BACKOFF_RETRIES;
        }

        $errors->addAll($validator->validate($options['backoffMaxTries'], [new Range(['min' => 0, 'max' => 100])]));
        $options['backoffMaxTries'] = (int) $options['backoffMaxTries'];

        if (empty($options['userAgent'])) {
            $options['userAgent'] = self::DEFAULT_USER_AGENT;
        }
        if ($errors->count() !== 0) {
            $messages = '';
            /** @var ConstraintViolationInterface $error */
            foreach ($errors as $error) {
                $messages .= 'Value "' . $error->getInvalidValue() . '" is invalid: ' . $error->getMessage() . "\n";
            }
            throw new SyncActionsClientException('Invalid parameters when creating client: ' . $messages);
        }
        $this->guzzle = $this->initClient($publicApiUrl, $storageToken, $options);
    }

    public function callAction(ActionData $actionData): array
    {
        try {
            $jobDataJson = json_encode($actionData->getArray(), JSON_THROW_ON_ERROR);
            $request = new Request('POST', 'actions', [], $jobDataJson);
        } catch (JsonException $e) {
            throw new SyncActionsClientException('Invalid job data: ' . $e->getMessage(), $e->getCode(), $e);
        }
        return $this->sendRequest($request);
    }

    public function getActions(string $componentId): array
    {
        $request = new Request('GET', sprintf('actions?componentId=%s', $componentId));
        return $this->sendRequest($request);
    }

    private function createDefaultDecider(int $maxRetries): Closure
    {
        return function (
            $retries,
            RequestInterface $request,
            ?ResponseInterface $response = null,
            $error = null,
        ) use ($maxRetries) {
            if ($retries >= $maxRetries) {
                return false;
            } elseif ($error && $error->getCode() >= 500) {
                return true;
            } else {
                return false;
            }
        };
    }

    private function initClient(string $url, string $token, array $options = []): GuzzleClient
    {
        // Initialize handlers (start with those supplied in constructor)
        $handlerStack = HandlerStack::create($options['handler'] ?? null);
        // Set exponential backoff
        $handlerStack->push(Middleware::retry($this->createDefaultDecider($options['backoffMaxTries'])));
        // Set handler to set default headers
        $handlerStack->push(Middleware::mapRequest(
            function (RequestInterface $request) use ($token, $options) {
                return $request
                    ->withHeader('User-Agent', $options['userAgent'])
                    ->withHeader('X-StorageApi-Token', $token)
                    ->withHeader('Content-type', 'application/json');
            },
        ));
        // Set client logger
        if (isset($options['logger']) && $options['logger'] instanceof LoggerInterface) {
            $handlerStack->push(Middleware::log(
                $options['logger'],
                new MessageFormatter(
                    '{hostname} {req_header_User-Agent} - [{ts}] "{method} {resource} {protocol}/{version}"' .
                    ' {code} {res_header_Content-Length}',
                ),
            ));
        }
        // finally create the instance
        return new GuzzleClient([
            'base_uri' => $url,
            'handler' => $handlerStack,
            'connect_timeout' => 10,
            'timeout' => 120,
        ]);
    }

    private function sendRequest(Request $request): array
    {
        $exception = null;
        try {
            $response = $this->guzzle->send($request);
        } catch (ClientException $exception) {
            $response = $exception->getResponse();
        } catch (GuzzleException $exception) {
            $response = null;
        }

        $data = null;
        if ($response !== null) {
            try {
                $data = (array) json_decode(
                    $response->getBody()->getContents(),
                    true,
                    self::JSON_DEPTH,
                    JSON_THROW_ON_ERROR,
                );
            } catch (JsonException $e) {
                throw new SyncActionsClientException(
                    'Unable to parse response body into JSON: ' . $e->getMessage(),
                    0,
                    $exception,
                );
            }
        }

        if ($exception === null) {
            return $data ?? [];
        }

        if ($exception instanceof ClientException) {
            throw new ResponseException($exception->getMessage(), $exception->getCode(), $data, $exception);
        }

        throw new SyncActionsClientException($exception->getMessage(), $exception->getCode(), $exception);
    }
}
