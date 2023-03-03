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
use Throwable;
use Webmozart\Assert\Assert;

class ApiClient
{
    private const USER_AGENT = 'Keboola Azure PHP Client';
    private const DEFAULT_BACKOFF_RETRIES = 10;

    private readonly HandlerStack $requestHandlerStack;
    private readonly GuzzleClient $httpClient;
    private readonly AuthenticatorInterface $authenticator;

    /**
     * @param non-empty-string|null $baseUrl
     * @param int<0, max>|null      $backoffMaxTries
     */
    public function __construct(
        ?string $baseUrl = null,
        ?int $backoffMaxTries = null,
        ?AuthenticatorInterface $authenticator = null,
        ?callable $requestHandler = null,
        ?LoggerInterface $logger = null,
    ) {
        $backoffMaxTries ??= self::DEFAULT_BACKOFF_RETRIES;
        $logger ??= new NullLogger();

        Assert::nullOrMinLength($baseUrl, 1);
        Assert::greaterThanEq($backoffMaxTries, 0);

        $this->authenticator = $authenticator ?? new SystemAuthenticatorResolver(
            backoffMaxTries: $backoffMaxTries,
            requestHandler: $requestHandler ? $requestHandler(...) : null,
            logger: $logger,
        );

        $this->requestHandlerStack = HandlerStack::create($requestHandler);

        if ($backoffMaxTries > 0) {
            $this->requestHandlerStack->push(Middleware::retry(new RetryDecider($backoffMaxTries, $logger)));
        }

        $this->requestHandlerStack->push(Middleware::log($logger, new MessageFormatter(
            '{hostname} {req_header_User-Agent} - [{ts}] "{method} {resource} {protocol}/{version}"' .
            ' {code} {res_header_Content-Length}'
        )));

        $this->httpClient = new GuzzleClient([
            'base_uri' => $baseUrl,
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
