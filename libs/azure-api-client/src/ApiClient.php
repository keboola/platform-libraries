<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient;

use GuzzleHttp\Client as GuzzleClient;
use GuzzleHttp\Exception\GuzzleException;
use GuzzleHttp\Exception\RequestException;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\MessageFormatter;
use GuzzleHttp\Middleware;
use JsonException;
use Keboola\AzureApiClient\Authentication\Authenticator\AuthenticatorInterface;
use Keboola\AzureApiClient\Authentication\Authenticator\SystemAuthenticatorResolver;
use Keboola\AzureApiClient\Authentication\AuthorizationHeaderResolver;
use Keboola\AzureApiClient\Exception\ClientException;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use Symfony\Component\Validator\Constraints as Assert;
use Symfony\Component\Validator\ConstraintViolationInterface;
use Symfony\Component\Validator\Validation;
use Throwable;

class ApiClient
{
    private const USER_AGENT = 'Keboola Azure PHP Client';
    private const DEFAULT_BACKOFF_RETRIES = 10;

    private readonly HandlerStack $requestHandlerStack;
    private readonly GuzzleClient $httpClient;
    private readonly AuthenticatorInterface $authenticator;
    private readonly LoggerInterface $logger;

    /**
     * @param array{
     *     baseUrl?: null|non-empty-string,
     *     backoffMaxTries?: null|int<0, max>,
     *     authenticator?: null|AuthenticatorInterface,
     *     requestHandler?: null|callable,
     *     logger?: null|LoggerInterface,
     * } $options
     */
    public function __construct(
        array $options = [],
    ) {
        $errors = Validation::createValidator()->validate($options, new Assert\Collection([
            'allowMissingFields' => true,
            'allowExtraFields' => false,
            'fields' => [
                'baseUrl' => new Assert\Sequentially([
                    new Assert\Url(),
                    new Assert\Length(['min' => 1]),
                ]),

                'backoffMaxTries' => new Assert\Sequentially([
                    new Assert\Type('int'),
                    new Assert\GreaterThanOrEqual(0),
                ]),

                'authenticator' => new Assert\Type(AuthenticatorInterface::class),

                'requestHandler' => new Assert\Type('callable'),

                'logger' => new Assert\Type(LoggerInterface::class),
            ],
        ]));

        if ($errors->count() > 0) {
            $messages = array_map(
                fn(ConstraintViolationInterface $error) => sprintf(
                    '%s: %s',
                    $error->getPropertyPath(),
                    $error->getMessage()
                ),
                iterator_to_array($errors),
            );

            throw new ClientException(sprintf('Invalid options when creating client: %s', implode("\n", $messages)));
        }

        $this->logger = $options['logger'] ?? new NullLogger();

        $this->authenticator = $options['authenticator'] ?? new SystemAuthenticatorResolver([
            'backoffMaxTries' => $options['backoffMaxTries'] ?? null,
            'requestHandler' => $options['requestHandler'] ?? null,
            'logger' => $this->logger,
        ]);

        $this->requestHandlerStack = HandlerStack::create($options['requestHandler'] ?? null);

        $backoffMaxTries = $options['backoffMaxTries'] ?? self::DEFAULT_BACKOFF_RETRIES;
        if ($backoffMaxTries > 0) {
            $this->requestHandlerStack->push(Middleware::retry(new RetryDecider($backoffMaxTries, $this->logger)));
        }

        $this->requestHandlerStack->push(Middleware::log($this->logger, new MessageFormatter(
            '{hostname} {req_header_User-Agent} - [{ts}] "{method} {resource} {protocol}/{version}"' .
            ' {code} {res_header_Content-Length}'
        )));

        $this->httpClient = new GuzzleClient([
            'base_uri' => $options['baseUrl'] ?? null,
            'handler' => $this->requestHandlerStack,
            'headers' => [
                'User-Agent' => self::USER_AGENT,
            ],
            'connect_timeout' => 10,
            'timeout' => 120,
        ]);
    }

    public function authenticate(string $resource): void
    {
        $middleware = Middleware::mapRequest(new AuthorizationHeaderResolver(
            $this->authenticator,
            $resource
        ));

        $this->requestHandlerStack->remove('auth');
        $this->requestHandlerStack->push($middleware, 'auth');
    }

    /**
     * @template TResponseClass of ResponseModelInterface
     * @param class-string<TResponseClass> $responseClass
     * @return ($isList is true ? list<TResponseClass> : TResponseClass)
     */
    public function sendRequestAndMapResponse(
        RequestInterface $request,
        string $responseClass,
        array $options = [],
        bool $isList = false,
    ) {
        $response = $this->doSendRequest($request, $options);

        try {
            $responseData = Json::decodeArray($response->getBody()->getContents());
        } catch (JsonException $e) {
            throw new ClientException('Response is not a valid JSON: ' . $e->getMessage(), $e->getCode(), $e);
        }

        try {
            if ($isList) {
                return array_map($responseClass::fromResponseData(...), $responseData);
            } else {
                return $responseClass::fromResponseData($responseData);
            }
        } catch (Throwable $e) {
            throw new ClientException('Failed to map response data: ' . $e->getMessage(), 0, $e);
        }
    }

    public function sendRequest(RequestInterface $request): void
    {
        $this->doSendRequest($request);
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
            $data = Json::decodeArray($response->getBody()->getContents());
        } catch (JsonException) {
            // throw the original one, we don't care about e2
            return new ClientException(trim($e->getMessage()), $response->getStatusCode(), $e);
        }

        $error = is_array($data['error'] ?? null) ? $data['error'] : $data;
        if (empty($error['message']) || empty($error['code'])) {
            return null;
        }

        return new ClientException(
            trim($error['code'] . ': ' . $error['message']),
            $response->getStatusCode(),
            $e
        );
    }
}
