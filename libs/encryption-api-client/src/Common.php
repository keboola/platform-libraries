<?php

declare(strict_types=1);

namespace Keboola\EncryptionApiClient;

use Closure;
use GuzzleHttp\BodySummarizer;
use GuzzleHttp\Client;
use GuzzleHttp\Exception\ConnectException;
use GuzzleHttp\Exception\GuzzleException;
use GuzzleHttp\Exception\RequestException;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Middleware;
use GuzzleHttp\Psr7\Request;
use InvalidArgumentException;
use JsonException;
use Keboola\EncryptionApiClient\Exception\ClientException;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;
use Throwable;

class Common
{
    protected Client $client;
    private const DEFAULT_BACKOFF_RETRIES = 10;
    private const DEFAULT_HEADERS = ['Accept' => 'application/json'];
    private const MAX_HTTP_ERROR_MESSAGE_LENGTH = 1024^2;

    /**
     * @throws ClientException
     */
    public function __construct(array $headers, array $config)
    {
        // Initialize handlers (start with those supplied in constructor)
        $handlerStack = $config['handler'] ?? HandlerStack::create();

        $handlerStack->push(
            Middleware::retry(
                self::createDefaultDecider($config['backoffMaxTries'] ?? self::DEFAULT_BACKOFF_RETRIES),
            ),
        );

        if (class_exists('GuzzleHttp\BodySummarizer')) {
            $handlerStack->remove('http_errors');
            $handlerStack->unshift(
                Middleware::httpErrors(new BodySummarizer(self::MAX_HTTP_ERROR_MESSAGE_LENGTH)),
                'http_errors',
            );
        }

        if (!isset($config['url'])) {
            throw new InvalidArgumentException('url must be set');
        }

        $this->client = new Client(
            [
                'base_uri' => $config['url'],
                'headers' => array_merge(
                    $headers,
                    self::DEFAULT_HEADERS,
                ),
                'handler' => $handlerStack,
            ],
        );
    }

    protected function apiGet(string $url): array
    {
        $request = new Request('GET', $url);
        return $this->sendRequest($request);
    }

    protected function apiDelete(string $url): void
    {
        $request = new Request('DELETE', $url);
        $this->sendRequest($request);
    }

    protected function apiPost(string $url, array $body): array
    {
        $request = new Request(
            'POST',
            $url,
            [
                'Content-Type' => 'application/json',
            ],
            json_encode($body, JSON_THROW_ON_ERROR),
        );
        return $this->sendRequest($request);
    }

    protected function apiPatch(string $url, array $body): array
    {
        $request = new Request(
            'PATCH',
            $url,
            [
                'Content-Type' => 'application/json',
            ],
            json_encode($body, JSON_THROW_ON_ERROR),
        );
        return $this->sendRequest($request);
    }

    protected function sendRequest(Request $request): array
    {
        try {
            $response = $this->client->send($request);
            $body = $response->getBody()->getContents();
            if ($body === '') {
                return [];
            }
            return (array) json_decode($body, true, 512, JSON_THROW_ON_ERROR);
        } catch (RequestException $e) {
            $message = $e->getMessage();
            if ($e->getResponse()) {
                try {
                    $response = (array) json_decode(
                        $e->getResponse()->getBody()->getContents(),
                        true,
                        512,
                        JSON_THROW_ON_ERROR,
                    );
                    if (!empty($response['message'])) {
                        $message = $response['message'];
                    }
                } catch (Throwable $e) {
                }
            }
            throw new ClientException('Encryption API error: ' . $message, $e->getCode(), $e);
        } catch (GuzzleException $e) {
            throw new ClientException('Encryption API error: ' . $e->getMessage(), $e->getCode(), $e);
        } catch (JsonException $e) {
            throw new ClientException('Unable to parse response body into JSON: ' . $e->getMessage());
        }
    }

    private static function createDefaultDecider(int $maxRetries): Closure
    {
        return function (
            int $retries,
            RequestInterface $request,
            ?ResponseInterface $response = null,
            ?Throwable $error = null
        ) use ($maxRetries) {
            if ($retries >= $maxRetries) {
                return false;
            } elseif ($response && $response->getStatusCode() >= 500) {
                return true;
            } elseif ($error && $error->getCode() >= 500) {
                return true;
            } elseif ($error &&
                 (is_a($error, RequestException::class) || is_a($error, ConnectException::class)) &&
                  in_array($error->getHandlerContext()['errno'] ?? 0, [CURLE_RECV_ERROR, CURLE_SEND_ERROR], true)
            ) {
                return true;
            } else {
                return false;
            }
        };
    }
}
