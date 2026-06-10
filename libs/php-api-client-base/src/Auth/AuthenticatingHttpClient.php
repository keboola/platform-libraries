<?php

declare(strict_types=1);

namespace Keboola\ApiClientBase\Auth;

use Symfony\Component\HttpClient\DecoratorTrait;
use Symfony\Contracts\HttpClient\HttpClientInterface;
use Symfony\Contracts\HttpClient\ResponseInterface;

/**
 * Decorates a {@see HttpClientInterface}, merging the authenticator's headers into
 * every request's `headers` option.
 *
 * The authenticator is resolved on each {@see self::request()} call. When this
 * decorator is itself wrapped by {@see \Symfony\Component\HttpClient\RetryableHttpClient}
 * (which re-invokes the inner client per attempt), auth headers are therefore
 * re-resolved on every retry — letting file-/token-backed authenticators pick up
 * rotated credentials. Per-request `headers` win over auth headers on key collision.
 */
final class AuthenticatingHttpClient implements HttpClientInterface
{
    use DecoratorTrait;

    public function __construct(
        private readonly RequestAuthenticatorInterface $authenticator,
        HttpClientInterface $client,
    ) {
        $this->client = $client;
    }

    public function request(string $method, string $url, array $options = []): ResponseInterface
    {
        /** @var array<string, string> $requestHeaders */
        $requestHeaders = $options['headers'] ?? [];
        // Per-request headers win over auth headers on key collision (array-union keeps left).
        $options['headers'] = $requestHeaders + $this->authenticator->getAuthenticationHeaders();

        return $this->client->request($method, $url, $options);
    }
}
