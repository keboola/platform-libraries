<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient;

use GuzzleHttp\Client as GuzzleClient;
use GuzzleHttp\Exception\GuzzleException;
use GuzzleHttp\Exception\RequestException;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Middleware;
use GuzzleHttp\Psr7\Request;
use JsonException;
use Keboola\AzureApiClient\Authentication\AuthenticatorFactory;
use Keboola\AzureApiClient\Authentication\AuthenticatorInterface;
use Keboola\AzureApiClient\Exception\ClientException;
use Psr\Http\Message\RequestInterface;
use Psr\Log\LoggerInterface;

class AzureApiClient
{
    private readonly GuzzleClient $guzzle;
    private readonly AuthenticatorInterface $authenticator;
    private readonly LoggerInterface $logger;
    private string $token;

    public function __construct(
        string $baseUrl,
        private readonly string $resource,
        GuzzleClientFactory $clientFactory,
        AuthenticatorFactory $authenticatorFactory,
        LoggerInterface $logger,
        array $options = [],
    ) {
        $handlerStack = $options['handler'] ?? HandlerStack::create();
        $options['handler'] = $handlerStack;

        // Set handler to set authorization
        $handlerStack->push(Middleware::mapRequest(
            function (RequestInterface $request) {
                return $request
                    ->withHeader('Authorization', 'Bearer ' . $this->token);
            }
        ));

        $this->guzzle = $clientFactory->getClient($baseUrl, $options);
        $this->authenticator = $authenticatorFactory->getAuthenticator($clientFactory);
        $this->logger = $logger;
    }

    /**
     * @return ($expectResponse is true ? array : null)
     */
    public function sendRequest(Request $request, bool $expectResponse = true): ?array
    {
        try {
            if (empty($this->token)) {
                $this->token = $this->authenticator->getAuthenticationToken($this->resource);
                $this->logger->info('Successfully authenticated.');
            }
            $response = $this->guzzle->send($request);

            if (!$expectResponse) {
                return null;
            }

            return (array) json_decode($response->getBody()->getContents(), true, 512, JSON_THROW_ON_ERROR);
        } catch (JsonException | GuzzleException $e) {
            if ($e instanceof RequestException) {
                $this->handleRequestException($e);
            }

            throw new ClientException($e->getMessage(), $e->getCode(), $e);
        }
    }

    private function handleRequestException(RequestException $e): void
    {
        $response = $e->getResponse();

        if ($response === null) {
            return;
        }

        try {
            $data = (array) json_decode($response->getBody()->getContents(), true, 512, JSON_THROW_ON_ERROR);
        } catch (JsonException $e2) {
            // throw the original one, we don't care about e2
            throw new ClientException(trim($e->getMessage()), $response->getStatusCode(), $e);
        }

        $error = is_array($data['error'] ?? null) ? $data['error'] : $data;

        if (!empty($error['message']) && !empty($error['code'])) {
            throw new ClientException(
                trim($error['code'] . ': ' . $error['message']),
                $response->getStatusCode(),
                $e
            );
        }
    }
}
